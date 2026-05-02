import type { FastifyRequest, FastifyReply, RouteHandlerMethod } from 'fastify'
import { proxyRequest } from '../proxy-request.ts'
import { drainAndReply } from '../drain-and-reply.ts'
import type { Registry } from '../registry.ts'

export type LookupAndDeregisterResult =
  | 'deregistered'
  | 'deregistered_dead_pod'
  | 'not_found'
  | 'upstream_error'

export interface LookupAndDeregisterOptions {
  instanceFrom: (req: FastifyRequest) => string
  expectedStatus?: number
  notFoundMessage?: string
  onResult?: (result: LookupAndDeregisterResult) => void
}

export function lookupAndDeregister (
  registry: Registry,
  opts: LookupAndDeregisterOptions
): RouteHandlerMethod {
  const {
    instanceFrom,
    expectedStatus = 204,
    notFoundMessage = 'Instance not found',
    onResult
  } = opts

  return async function (request: FastifyRequest, reply: FastifyReply) {
    const instanceId = instanceFrom(request)
    const resolved = await registry.resolveInstance(instanceId)

    if (!resolved) {
      onResult?.('not_found')
      return reply.code(404).send({ error: notFoundMessage })
    }

    if (resolved.address === null) {
      await registry.deregisterInstance(instanceId)
      onResult?.('deregistered_dead_pod')
      return reply.code(expectedStatus).send()
    }

    const upstream = await proxyRequest(resolved.address, request, { timeout: registry.requestTimeout })

    if (upstream.statusCode === expectedStatus) {
      await upstream.body.dump()
      await registry.deregisterInstance(instanceId)
      onResult?.('deregistered')
      return reply.code(expectedStatus).send()
    }

    onResult?.('upstream_error')
    return drainAndReply(reply, upstream)
  }
}
