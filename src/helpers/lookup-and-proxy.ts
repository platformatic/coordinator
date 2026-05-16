import type { FastifyRequest, FastifyReply, RouteHandlerMethod } from 'fastify'
import '@fastify/reply-from'
import type { Registry } from '../registry.ts'

export type LookupAndProxyResult = 'hit' | 'orphan_reassigned' | 'not_found'

export interface LookupAndProxyOptions {
  instanceFrom: (req: FastifyRequest) => string
  reassignOrphans?: boolean
  claimOnMiss?: boolean
  notFoundMessage?: string
  onResult?: (result: LookupAndProxyResult) => void
}

export function lookupAndProxy (
  registry: Registry,
  opts: LookupAndProxyOptions
): RouteHandlerMethod {
  const {
    instanceFrom,
    reassignOrphans = false,
    claimOnMiss = false,
    notFoundMessage = 'Instance not found',
    onResult
  } = opts

  return async function (request: FastifyRequest, reply: FastifyReply) {
    const instanceId = instanceFrom(request)
    const resolved = await registry.resolveInstance(instanceId, { reassignOrphans, claimOnMiss })

    if (!resolved) {
      onResult?.('not_found')
      return reply.code(404).send({ error: notFoundMessage })
    }

    onResult?.(resolved.reassigned ? 'orphan_reassigned' : 'hit')
    return reply.from(`${resolved.address}${request.url}`)
  }
}
