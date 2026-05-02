import { request as undiciRequest, type Dispatcher } from 'undici'
import type { FastifyRequest } from 'fastify'

export interface ProxyRequestOptions {
  timeout?: number
  upstreamPath?: string
}

export async function proxyRequest (
  address: string,
  req: FastifyRequest,
  opts: ProxyRequestOptions = {}
): Promise<Dispatcher.ResponseData<null>> {
  const path = opts.upstreamPath ?? req.url
  const hasBody = req.body !== undefined && req.body !== null
  return undiciRequest(`${address}${path}`, {
    method: req.method as Dispatcher.HttpMethod,
    headersTimeout: opts.timeout,
    bodyTimeout: opts.timeout,
    body: hasBody ? JSON.stringify(req.body) : undefined,
    headers: hasBody ? { 'content-type': 'application/json' } : undefined
  })
}
