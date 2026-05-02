import type { FastifyRequest, FastifyReply, RouteHandlerMethod } from 'fastify'
import { proxyRequest } from '../proxy-request.ts'
import { drainAndReply } from '../drain-and-reply.ts'
import type { Registry } from '../registry.ts'

export interface LookupAndProxyOptions {
  resourceFrom: (req: FastifyRequest) => string
  reassignOrphans?: boolean
  notFoundMessage?: string
}

export function lookupAndProxy (
  registry: Registry,
  opts: LookupAndProxyOptions
): RouteHandlerMethod {
  const {
    resourceFrom,
    reassignOrphans = false,
    notFoundMessage = 'Resource not found'
  } = opts

  return async function (request: FastifyRequest, reply: FastifyReply) {
    const resourceId = resourceFrom(request)
    const route = request.routeOptions?.url ?? request.url
    const resolved = await registry.resolveResource(resourceId, { reassignOrphans })

    if (!resolved || resolved.address === null) {
      registry.metrics?.requestsTotal.inc({ route, result: 'not_found' })
      return reply.code(404).send({ error: notFoundMessage })
    }

    registry.metrics?.requestsTotal.inc({
      route,
      result: resolved.reassigned ? 'orphan_reassigned' : 'hit'
    })

    const upstream = await proxyRequest(resolved.address, request, { timeout: registry.requestTimeout })
    return drainAndReply(reply, upstream)
  }
}
