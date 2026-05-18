import type { FastifyRequest, RouteHandlerMethod } from 'fastify'
import '@fastify/reply-from'
import type { Registry } from '../registry.ts'
import { proxyVia } from './proxy-via.ts'

export type LookupLockAndProxyResult = 'hit' | 'not_found'

export interface LookupLockAndProxyOptions {
  lockFrom: (req: FastifyRequest) => string
  notFoundMessage?: string
  onResult?: (result: LookupLockAndProxyResult) => void
}

export function lookupLockAndProxy (
  registry: Registry,
  opts: LookupLockAndProxyOptions
): RouteHandlerMethod {
  const {
    lockFrom,
    notFoundMessage = 'Lock not found',
    onResult
  } = opts

  return proxyVia(async (request) => {
    const lockId = lockFrom(request)
    const resolved = await registry.resolveLock(lockId)
    if (!resolved) {
      onResult?.('not_found')
      return null
    }
    onResult?.('hit')
    return resolved
  }, { notFoundMessage })
}
