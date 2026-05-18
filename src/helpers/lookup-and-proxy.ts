import type { FastifyRequest, RouteHandlerMethod } from 'fastify'
import '@fastify/reply-from'
import type { Registry } from '../registry.ts'
import { proxyVia } from './proxy-via.ts'

export type LookupAndProxyResult = 'hit' | 'orphan_reassigned' | 'not_found'

export interface LookupAndProxyOptions {
  destinationFrom: (req: FastifyRequest) => string
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
    destinationFrom,
    reassignOrphans = false,
    claimOnMiss = false,
    notFoundMessage = 'Destination not found',
    onResult
  } = opts

  return proxyVia(async (request) => {
    const destinationId = destinationFrom(request)
    const resolved = await registry.resolveDestination(destinationId, { reassignOrphans, claimOnMiss })
    if (!resolved) {
      onResult?.('not_found')
      return null
    }
    onResult?.(resolved.reassigned ? 'orphan_reassigned' : 'hit')
    return resolved
  }, { notFoundMessage })
}
