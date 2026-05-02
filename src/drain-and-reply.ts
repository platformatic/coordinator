import type { FastifyReply } from 'fastify'
import type { Dispatcher } from 'undici'

export async function drainAndReply (
  reply: FastifyReply,
  upstream: Dispatcher.ResponseData<null>
): Promise<unknown> {
  reply.code(upstream.statusCode)
  if (upstream.statusCode === 204) {
    await upstream.body.dump()
    return reply.send()
  }
  return upstream.body.json()
}
