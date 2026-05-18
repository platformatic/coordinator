import type { FastifyRequest, FastifyReply, RouteHandlerMethod } from 'fastify'
import '@fastify/reply-from'

export interface ProxyTarget {
  address: string
}

export type ProxyResolver<T extends ProxyTarget = ProxyTarget> =
  (request: FastifyRequest) => Promise<T | null>

export interface ProxyViaOptions {
  notFoundMessage?: string
}

export function proxyVia<T extends ProxyTarget = ProxyTarget> (
  resolve: ProxyResolver<T>,
  opts: ProxyViaOptions = {}
): RouteHandlerMethod {
  const notFoundMessage = opts.notFoundMessage ?? 'Not found'
  return async function (request: FastifyRequest, reply: FastifyReply) {
    const resolved = await resolve(request)
    if (!resolved) return reply.code(404).send({ error: notFoundMessage })
    return reply.from(`${resolved.address}${request.url}`)
  }
}
