import { Buffer } from 'node:buffer'
import type { FastifyRequest, FastifyReply, RouteHandlerMethod } from 'fastify'
import type { FastifyReplyFromHooks } from '@fastify/reply-from'
import '@fastify/reply-from'
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

    const onResponse: FastifyReplyFromHooks['onResponse'] = (_req, replyOut, res) => {
      const chunks: Buffer[] = []
      res.stream.on('data', (c: Buffer) => chunks.push(c))
      res.stream.on('end', () => {
        const raw = Buffer.concat(chunks).toString('utf8')
        let body: any
        try {
          body = raw.length > 0 ? JSON.parse(raw) : null
        } catch {
          onResult?.('upstream_error')
          replyOut.send(raw)
          return
        }

        if (res.statusCode === expectedStatus) {
          const id = registerIdFrom(body)
          registry.registerInstance(id, member.memberId).then(
            () => {
              onResult?.('spawned')
              replyOut.send(body)
            },
            (err) => replyOut.send(err)
          )
        } else {
          onResult?.('upstream_error')
          replyOut.send(body)
        }
      })
      res.stream.on('error', (err: Error) => replyOut.send(err))
    }

    return reply.from(`${member.address}${request.url}`, { onResponse })
  }
}
