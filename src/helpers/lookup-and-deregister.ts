import type { FastifyRequest, FastifyReply, RouteHandlerMethod } from 'fastify'
import type { FastifyReplyFromHooks } from '@fastify/reply-from'
import '@fastify/reply-from'
import type { Registry } from '../registry.ts'

export type LookupAndDeregisterResult =
  | 'deregistered'
  | 'deregistered_dead_pod'
  | 'not_found'
  | 'upstream_error'

export interface LookupAndDeregisterOptions {
  destinationFrom: (req: FastifyRequest) => string
  expectedStatus?: number
  notFoundMessage?: string
  onResult?: (result: LookupAndDeregisterResult) => void
}

export function lookupAndDeregister (
  registry: Registry,
  opts: LookupAndDeregisterOptions
): RouteHandlerMethod {
  const {
    destinationFrom,
    expectedStatus = 204,
    notFoundMessage = 'Destination not found',
    onResult
  } = opts

  return async function (request: FastifyRequest, reply: FastifyReply) {
    const destinationId = destinationFrom(request)
    const resolved = await registry.resolveDestination(destinationId)

    if (!resolved) {
      // No live pod for this destination. Distinguish "binding exists but pods dead"
      // from "destination unknown".
      const exists = await registry.hasBinding(destinationId)
      if (exists) {
        await registry.deregisterDestination(destinationId)
        onResult?.('deregistered_dead_pod')
        return reply.code(expectedStatus).send()
      }
      onResult?.('not_found')
      return reply.code(404).send({ error: notFoundMessage })
    }

    const onResponse: FastifyReplyFromHooks['onResponse'] = (_req, replyOut, res) => {
      if (res.statusCode === expectedStatus) {
        res.stream.resume()
        registry.deregisterDestination(destinationId).then(
          () => {
            onResult?.('deregistered')
            replyOut.send()
          },
          (err: Error) => replyOut.send(err)
        )
      } else {
        onResult?.('upstream_error')
        replyOut.send(res.stream)
      }
    }

    return reply.from(`${resolved.address}${request.url}`, { onResponse })
  }
}
