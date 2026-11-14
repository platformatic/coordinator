import { Buffer } from 'node:buffer'
import type { FastifyRequest, FastifyReply, RouteHandlerMethod } from 'fastify'
import type { FastifyReplyFromHooks } from '@fastify/reply-from'
import '@fastify/reply-from'
import type { Registry } from '../registry.ts'

export type PickAndRegisterResult = 'spawned' | 'unavailable' | 'upstream_error' | 'refused'

export interface PickAndRegisterOptions {
  registerIdFrom: (responseBody: any) => string
  expectedStatus?: number
  unavailableMessage?: string
  /** The message of the 503 sent when the pod started draining after it was picked, so the binding was refused. */
  refusedMessage?: string
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
    refusedMessage = 'The pod started draining, retry',
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
          registry.addPodToDestination(id, member.memberId).then(
            (bound) => {
              // The pod started draining between the pick and the binding: the resource it created is not routable,
              // the caller retries on another pod
              if (!bound) {
                onResult?.('refused')
                replyOut.code(503).send({ error: refusedMessage })
                return
              }

              onResult?.('spawned')
              replyOut.send(body)
            },
            (err: Error) => replyOut.send(err)
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
