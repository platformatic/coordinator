import type { FastifyRequest, FastifyReply, RouteHandlerMethod } from 'fastify'
import { proxyRequest } from '../proxy-request.ts'
import { drainAndReply } from '../drain-and-reply.ts'
import type { Registry } from '../registry.ts'

export interface LookupAndDeregisterOptions {
  instanceFrom: (req: FastifyRequest) => string
  expectedStatus?: number
  notFoundMessage?: string
}

export function lookupAndDeregister (
  registry: Registry,
  opts: LookupAndDeregisterOptions
): RouteHandlerMethod {
  const {
    instanceFrom,
    expectedStatus = 204,
    notFoundMessage = 'Instance not found'
  } = opts

  return async function (request: FastifyRequest, reply: FastifyReply) {
    const instanceId = instanceFrom(request)
    const route = request.routeOptions?.url ?? request.url
    const resolved = await registry.resolveInstance(instanceId)

    if (!resolved) {
      registry.metrics?.requestsTotal.inc({ route, result: 'not_found' })
      return reply.code(404).send({ error: notFoundMessage })
    }

    if (resolved.address === null) {
      await registry.deregisterInstance(instanceId)
      registry.metrics?.requestsTotal.inc({ route, result: 'deregistered_dead_pod' })
      return reply.code(expectedStatus).send()
    }

    const upstream = await proxyRequest(resolved.address, request, { timeout: registry.requestTimeout })

    if (upstream.statusCode === expectedStatus) {
      await upstream.body.dump()
      await registry.deregisterInstance(instanceId)
      registry.metrics?.requestsTotal.inc({ route, result: 'deregistered' })
      return reply.code(expectedStatus).send()
    }

    registry.metrics?.requestsTotal.inc({ route, result: 'upstream_error' })
    return drainAndReply(reply, upstream)
  }
}
