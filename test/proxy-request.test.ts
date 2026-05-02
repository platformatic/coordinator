import { strictEqual, ok, deepStrictEqual } from 'node:assert'
import test from 'node:test'
import Fastify from 'fastify'
import { proxyRequest } from '../src/proxy-request.ts'

async function startUpstream (handler: (req: any, reply: any) => unknown): Promise<{ address: string, close: () => Promise<void> }> {
  const app = Fastify()
  app.all('*', handler as any)
  await app.listen({ port: 0, host: '127.0.0.1' })
  const addr = app.server.address() as any
  return {
    address: `http://127.0.0.1:${addr.port}`,
    close: () => app.close()
  }
}

test('proxyRequest forwards method, url, and JSON body', async (t) => {
  let captured: any = null
  const upstream = await startUpstream(async (req, reply) => {
    captured = {
      method: req.method,
      url: req.url,
      body: req.body,
      contentType: req.headers['content-type']
    }
    return reply.code(200).send({ ok: true })
  })

  t.after(() => upstream.close())

  // Build a fake FastifyRequest-shaped object
  const fakeReq = {
    method: 'POST',
    url: '/test/path?x=1',
    body: { hello: 'world' }
  } as any

  const res = await proxyRequest(upstream.address, fakeReq)
  strictEqual(res.statusCode, 200)
  const body = await res.body.json() as any
  deepStrictEqual(body, { ok: true })

  strictEqual(captured.method, 'POST')
  strictEqual(captured.url, '/test/path?x=1')
  deepStrictEqual(captured.body, { hello: 'world' })
  ok(captured.contentType.startsWith('application/json'))
})

test('proxyRequest does not send a body when req.body is null/undefined', async (t) => {
  let captured: any = null
  const upstream = await startUpstream(async (req, reply) => {
    captured = {
      method: req.method,
      bodyPresent: req.body !== null && req.body !== undefined && Object.keys(req.body || {}).length > 0,
      contentType: req.headers['content-type']
    }
    return reply.code(204).send()
  })

  t.after(() => upstream.close())

  const fakeReq = { method: 'GET', url: '/get-thing', body: undefined } as any
  const res = await proxyRequest(upstream.address, fakeReq)
  strictEqual(res.statusCode, 204)
  await res.body.dump()

  strictEqual(captured.method, 'GET')
  strictEqual(captured.bodyPresent, false)
  strictEqual(captured.contentType, undefined)
})

test('proxyRequest honors upstreamPath override', async (t) => {
  let capturedUrl: string | null = null
  const upstream = await startUpstream(async (req, reply) => {
    capturedUrl = req.url
    return reply.code(200).send({ ok: true })
  })

  t.after(() => upstream.close())

  const fakeReq = { method: 'GET', url: '/original', body: undefined } as any
  const res = await proxyRequest(upstream.address, fakeReq, { upstreamPath: '/override' })
  await res.body.json()
  strictEqual(capturedUrl, '/override')
})
