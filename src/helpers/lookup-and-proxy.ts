import type { FastifyRequest, FastifyReply, RouteHandlerMethod } from 'fastify'
import { proxyRequest } from '../proxy-request.ts'
import { drainAndReply } from '../drain-and-reply.ts'
import type { Registry } from '../registry.ts'

export interface LookupAndProxyOptions {
  instanceFrom: (req: FastifyRequest) => string
  reassignOrphans?: boolean
  notFoundMessage?: string
}

export function lookupAndProxy (
  registry: Registry,
  opts: LookupAndProxyOptions
): RouteHandlerMethod {
  const {
    instanceFrom,
    reassignOrphans = false,
    notFoundMessage = 'Instance not found'
  } = opts

  return async function (request: FastifyRequest, reply: FastifyReply) {
    const instanceId = instanceFrom(request)
    const route = request.routeOptions?.url ?? request.url
    const resolved = await registry.resolveInstance(instanceId, { reassignOrphans })

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
