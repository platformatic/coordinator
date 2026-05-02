import type { FastifyRequest, FastifyReply, RouteHandlerMethod } from 'fastify'
import { proxyRequest } from '../proxy-request.ts'
import { drainAndReply } from '../drain-and-reply.ts'
import type { Registry } from '../registry.ts'

export interface LookupAndDeregisterOptions {
  resourceFrom: (req: FastifyRequest) => string
  expectedStatus?: number
  notFoundMessage?: string
}

export function lookupAndDeregister (
  registry: Registry,
  opts: LookupAndDeregisterOptions
): RouteHandlerMethod {
  const {
    resourceFrom,
    expectedStatus = 204,
    notFoundMessage = 'Resource not found'
  } = opts

  return async function (request: FastifyRequest, reply: FastifyReply) {
    const resourceId = resourceFrom(request)
    const route = request.routeOptions?.url ?? request.url
    const resolved = await registry.resolveResource(resourceId)

    if (!resolved) {
      registry.metrics?.requestsTotal.inc({ route, result: 'not_found' })
      return reply.code(404).send({ error: notFoundMessage })
    }

    if (resolved.address === null) {
      await registry.deregisterResource(resourceId)
      registry.metrics?.requestsTotal.inc({ route, result: 'deregistered_dead_pod' })
      return reply.code(expectedStatus).send()
    }

    const upstream = await proxyRequest(resolved.address, request, { timeout: registry.requestTimeout })

    if (upstream.statusCode === expectedStatus) {
      await upstream.body.dump()
      await registry.deregisterResource(resourceId)
      registry.metrics?.requestsTotal.inc({ route, result: 'deregistered' })
      return reply.code(expectedStatus).send()
    }

    registry.metrics?.requestsTotal.inc({ route, result: 'upstream_error' })
    return drainAndReply(reply, upstream)
  }
}
