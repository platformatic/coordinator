import type { FastifyRequest, FastifyReply, RouteHandlerMethod } from 'fastify'
import { proxyRequest } from '../proxy-request.ts'
import type { Registry } from '../registry.ts'

export interface PickAndRegisterOptions {
  registerIdFrom: (responseBody: any) => string
  expectedStatus?: number
  unavailableMessage?: string
}

export function pickAndRegister (
  registry: Registry,
  opts: PickAndRegisterOptions
): RouteHandlerMethod {
  const {
    registerIdFrom,
    expectedStatus = 201,
    unavailableMessage = 'No pods available'
  } = opts

  return async function (request: FastifyRequest, reply: FastifyReply) {
    const route = request.routeOptions?.url ?? request.url
    const member = await registry.pickMember()
    if (!member) {
      registry.metrics?.requestsTotal.inc({ route, result: 'unavailable' })
      return reply.code(503).send({ error: unavailableMessage })
    }

    const upstream = await proxyRequest(member.address, request, { timeout: registry.requestTimeout })
    const body = await upstream.body.json() as any

    if (upstream.statusCode === expectedStatus) {
      const id = registerIdFrom(body)
      await registry.registerResource(id, member.memberId)
      registry.metrics?.requestsTotal.inc({ route, result: 'spawned' })
    } else {
      registry.metrics?.requestsTotal.inc({ route, result: 'upstream_error' })
    }

    return reply.code(upstream.statusCode).send(body)
  }
}
