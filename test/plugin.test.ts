import { strictEqual, ok } from 'node:assert'
import { randomBytes } from 'node:crypto'
import test from 'node:test'
import Fastify from 'fastify'
import { Redis } from 'iovalkey'
import coordinatorPlugin from '../src/plugin.ts'
import { Registry } from '../src/registry.ts'
import { REDIS_URL } from './redis-url.ts'

const PREFIX = `test-plugin-${randomBytes(4).toString('hex')}`

const membersKey = (): string => `${PREFIX}:members`
const memberKey = (id: string): string => `${PREFIX}:member:${id}`
const destinationKey = (id: string): string => `${PREFIX}:destination:${id}`
const lockKey = (id: string): string => `${PREFIX}:lock:${id}`

async function makeLivePod (redis: Redis, memberId: string, address: string): Promise<void> {
  await redis.sadd(membersKey(), memberId)
  await redis.hset(memberKey(memberId), { address, load: '0' })
  await redis.expire(memberKey(memberId), 60)
}

interface MockPod { app: ReturnType<typeof Fastify>, address: string }

async function createMockPod (): Promise<MockPod> {
  const app = Fastify()

  app.get('/items/:id', async (req) => ({ id: (req.params as any).id, served: true }))
  app.post('/resources', async (req, reply) => {
    return reply.code(201).send({ resourceId: `r-${randomBytes(3).toString('hex')}` })
  })
  app.delete('/items/:id', async (req, reply) => reply.code(204).send())
  app.post('/transactions/:lockId/echo', async (req) => ({ lockId: (req.params as any).lockId }))

  await app.listen({ port: 0, host: '127.0.0.1' })
  const addr = app.server.address() as any
  return { app, address: `http://127.0.0.1:${addr.port}` }
}

test('coordinatorPlugin', async (t) => {
  const redis = new Redis(REDIS_URL)
  const pod = await createMockPod()
  await makeLivePod(redis, 'pod-1', pod.address)

  t.after(async () => {
    await pod.app.close()
    const stream = redis.scanStream({ match: `${PREFIX}:*`, count: 100 })
    for await (const keys of stream) {
      if (keys.length > 0) await redis.del(...keys)
    }
    await redis.quit()
  })

  await t.test('registers reply-from and decorates the Coordinator', async () => {
    const app = Fastify()
    await app.register(coordinatorPlugin, { redis: REDIS_URL, keyPrefix: PREFIX, cache: false })

    ok(app.hasReplyDecorator('from'), 'reply.from should be available')
    ok(app.coordinator.registry instanceof Registry, 'app.coordinator.registry should be a Registry')
    ok(typeof app.coordinator.lookupAndProxy === 'function')
    ok(typeof app.coordinator.lookupLockAndProxy === 'function')
    ok(typeof app.coordinator.pickAndRegister === 'function')
    ok(typeof app.coordinator.lookupAndDeregister === 'function')
    ok(typeof app.coordinator.proxyVia === 'function')

    await app.close()
  })

  await t.test('lookupAndProxy via decorator routes a request', async () => {
    const app = Fastify()
    await app.register(coordinatorPlugin, { redis: REDIS_URL, keyPrefix: PREFIX, cache: false })

    app.get('/items/:id', app.coordinator.lookupAndProxy({
      destinationFrom: (req) => (req.params as any).id,
      reassignOrphans: true,
      claimOnMiss: true
    }))

    const res = await app.inject({ method: 'GET', url: '/items/abc' })
    strictEqual(res.statusCode, 200)
    const body = res.json() as any
    strictEqual(body.id, 'abc')
    strictEqual(body.served, true)

    const set = await redis.smembers(destinationKey('abc'))
    strictEqual(set.length, 1)
    strictEqual(set[0], 'pod-1')

    await app.close()
  })

  await t.test('pickAndRegister via decorator binds to a pod', async () => {
    const app = Fastify()
    await app.register(coordinatorPlugin, { redis: REDIS_URL, keyPrefix: PREFIX, cache: false })

    app.post('/resources', app.coordinator.pickAndRegister({
      registerIdFrom: (res: any) => res.resourceId
    }))

    const res = await app.inject({ method: 'POST', url: '/resources' })
    strictEqual(res.statusCode, 201)
    const body = res.json() as any
    ok(body.resourceId)
    const set = await redis.smembers(destinationKey(body.resourceId))
    strictEqual(set.length, 1)

    await app.close()
  })

  await t.test('lookupLockAndProxy via decorator routes by lockId', async () => {
    const lockId = `lock-${randomBytes(3).toString('hex')}`
    await redis.hset(lockKey(lockId), { podId: 'pod-1', destinationId: 'tenant-x' })

    const app = Fastify()
    await app.register(coordinatorPlugin, { redis: REDIS_URL, keyPrefix: PREFIX, cache: false })

    app.post('/transactions/:lockId/echo', app.coordinator.lookupLockAndProxy({
      lockFrom: (req) => (req.params as any).lockId
    }))

    const res = await app.inject({ method: 'POST', url: `/transactions/${lockId}/echo` })
    strictEqual(res.statusCode, 200)
    strictEqual((res.json() as any).lockId, lockId)

    await app.close()
  })

  await t.test('accepts an existing Registry and does not close it', async () => {
    const registry = new Registry({ redis: REDIS_URL, keyPrefix: PREFIX, cache: false })
    const app = Fastify()
    await app.register(coordinatorPlugin, { registry })

    strictEqual(app.coordinator.registry, registry)
    await app.close()

    const live = await registry.listLiveMembers()
    ok(live.length >= 1)
    await registry.close()
  })

  await t.test('decorateAs renames the decorator', async () => {
    const app = Fastify()
    await app.register(coordinatorPlugin, {
      redis: REDIS_URL, keyPrefix: PREFIX, cache: false, decorateAs: 'router'
    })
    ok((app as any).router?.registry instanceof Registry)
    strictEqual((app as any).coordinator, undefined)
    await app.close()
  })

  await t.test('registerReplyFrom=false skips reply-from registration', async () => {
    const registry = new Registry({ redis: REDIS_URL, keyPrefix: PREFIX, cache: false })
    const app = Fastify()
    await app.register(coordinatorPlugin, { registry, registerReplyFrom: false })
    strictEqual(app.hasReplyDecorator('from'), false)
    await app.close()
    await registry.close()
  })
})
