import type { FastifyRequest, FastifyReply, RouteHandlerMethod } from 'fastify'
import { proxyRequest } from '../proxy-request.ts'
import type { Registry } from '../registry.ts'

export type PickAndRegisterResult = 'spawned' | 'unavailable' | 'upstream_error'

export interface PickAndRegisterOptions {
  registerIdFrom: (responseBody: any) => string
  expectedStatus?: number
  unavailableMessage?: string
  onResult?: (result: PickAndRegisterResult) => void
}

export function pickAndRegister (
  registry: Registry,
  opts: PickAndRegisterOptions
): RouteHandlerMethod {
  const {
    registerIdFrom,
    expectedStatus = 201,
    unavailableMessage = 'No pods available',
    onResult
  } = opts

  return async function (request: FastifyRequest, reply: FastifyReply) {
    const member = await registry.pickMember()
    if (!member) {
      onResult?.('unavailable')
      return reply.code(503).send({ error: unavailableMessage })
    }

    const upstream = await proxyRequest(member.address, request, { timeout: registry.requestTimeout })
    const body = await upstream.body.json() as any

    if (upstream.statusCode === expectedStatus) {
      const id = registerIdFrom(body)
      await registry.registerInstance(id, member.memberId)
      onResult?.('spawned')
    } else {
      onResult?.('upstream_error')
    }

    return reply.code(upstream.statusCode).send(body)
  }
}
