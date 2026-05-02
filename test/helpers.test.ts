import { strictEqual, ok } from 'node:assert'
import { randomBytes } from 'node:crypto'
import test from 'node:test'
import Fastify from 'fastify'
import { Redis } from 'iovalkey'
import { Registry } from '../src/registry.ts'
import { lookupAndProxy } from '../src/helpers/lookup-and-proxy.ts'
import { pickAndRegister } from '../src/helpers/pick-and-register.ts'
import { lookupAndDeregister } from '../src/helpers/lookup-and-deregister.ts'
import { REDIS_URL } from './redis-url.ts'

const PREFIX = `test-${randomBytes(4).toString('hex')}`

function membersKey (): string { return `${PREFIX}:members` }
function memberKey (id: string): string { return `${PREFIX}:member:${id}` }
function resourceKey (id: string): string { return `${PREFIX}:instance:${id}` }

interface MockPod {
  app: ReturnType<typeof Fastify>
  address: string
  resources: Map<string, any>
}

async function createMockPod (): Promise<MockPod> {
  const resources = new Map<string, any>()
  const app = Fastify()

  app.post('/resources', async (req, reply) => {
    const id = `r-${randomBytes(3).toString('hex')}`
    const info = { resourceId: id, status: 'started' }
    resources.set(id, info)
    return reply.code(201).send(info)
  })

  app.post('/resources/:id/echo', async (req, reply) => {
    const { id } = req.params as { id: string }
    if (!resources.has(id)) return reply.code(404).send({ error: 'not found' })
    return reply.code(200).send({ id, body: req.body })
  })

  app.delete('/resources/:id', async (req, reply) => {
    const { id } = req.params as { id: string }
    if (!resources.has(id)) return reply.code(404).send({ error: 'not found' })
    resources.delete(id)
    return reply.code(204).send()
  })

  app.post('/resources/:id/heartbeat', async (req, reply) => {
    const { id } = req.params as { id: string }
    if (!resources.has(id)) {
      // Simulate auto-restore so orphan reassignment can succeed
      resources.set(id, { resourceId: id, status: 'restored' })
    }
    return reply.code(204).send()
  })

  await app.listen({ port: 0, host: '127.0.0.1' })
  const addr = app.server.address() as any
  return { app, address: `http://127.0.0.1:${addr.port}`, resources }
}

async function createCoordinator (registry: Registry) {
  const app = Fastify()

  app.post('/resources', pickAndRegister(registry, {
    registerIdFrom: (res: any) => res.resourceId
  }))

  app.post('/resources/:id/echo',
    { schema: { body: { type: 'object', properties: { msg: { type: 'string' } } } } },
    lookupAndProxy(registry, {
      instanceFrom: (req: any) => req.params.id,
      reassignOrphans: true
    }))

  app.post('/resources/:id/heartbeat', lookupAndProxy(registry, {
    instanceFrom: (req: any) => req.params.id,
    reassignOrphans: true
  }))

  app.delete('/resources/:id', lookupAndDeregister(registry, {
    instanceFrom: (req: any) => req.params.id
  }))

  return app
}

test('Coordinator helpers', async (t) => {
  const redis = new Redis(REDIS_URL)
  const memberId1 = 'pod-1'
  const memberId2 = 'pod-2'

  const pod1 = await createMockPod()
  const pod2 = await createMockPod()

  await redis.sadd(membersKey(), memberId1)
  await redis.set(memberKey(memberId1), pod1.address, 'EX', 60)
  await redis.sadd(membersKey(), memberId2)
  await redis.set(memberKey(memberId2), pod2.address, 'EX', 60)

  const registry = new Registry({ redis: REDIS_URL, keyPrefix: PREFIX })
  const coordinator = await createCoordinator(registry)

  t.after(async () => {
    await coordinator.close()
    await pod1.app.close()
    await pod2.app.close()
    const stream = redis.scanStream({ match: `${PREFIX}:*`, count: 100 })
    for await (const keys of stream) {
      if (keys.length > 0) await redis.del(...keys)
    }
    await registry.close()
    await redis.quit()
  })

  await t.test('pickAndRegister: spawns and registers in Redis', async () => {
    const res = await coordinator.inject({ method: 'POST', url: '/resources' })
    strictEqual(res.statusCode, 201)
    const body = res.json() as any
    ok(body.resourceId)

    const memberId = await redis.get(resourceKey(body.resourceId))
    ok(memberId === memberId1 || memberId === memberId2)
  })

  await t.test('pickAndRegister: returns 503 when no pods are available', async () => {
    const isolatedPrefix = `${PREFIX}-empty-${randomBytes(2).toString('hex')}`
    const emptyRegistry = new Registry({ redis: REDIS_URL, keyPrefix: isolatedPrefix })
    const app = Fastify()
    app.post('/spawn', pickAndRegister(emptyRegistry, { registerIdFrom: (r: any) => r.id }))

    try {
      const res = await app.inject({ method: 'POST', url: '/spawn' })
      strictEqual(res.statusCode, 503)
      const body = res.json() as any
      ok(body.error)
    } finally {
      await app.close()
      await emptyRegistry.close()
    }
  })

  await t.test('lookupAndProxy: routes to the registered pod', async () => {
    const spawnRes = await coordinator.inject({ method: 'POST', url: '/resources' })
    const id = (spawnRes.json() as any).resourceId

    const echoRes = await coordinator.inject({
      method: 'POST',
      url: `/resources/${id}/echo`,
      payload: { msg: 'hello' }
    })
    strictEqual(echoRes.statusCode, 200)
    const body = echoRes.json() as any
    strictEqual(body.id, id)
    strictEqual(body.body.msg, 'hello')
  })

  await t.test('lookupAndProxy: returns 404 for unknown resource', async () => {
    const res = await coordinator.inject({
      method: 'POST',
      url: '/resources/does-not-exist/echo',
      payload: { msg: 'x' }
    })
    strictEqual(res.statusCode, 404)
    const body = res.json() as any
    strictEqual(body.error, 'Instance not found')
  })

  await t.test('lookupAndProxy: reassigns orphan when reassignOrphans is true', async () => {
    const orphanId = `orphan-${randomBytes(3).toString('hex')}`
    const deadMemberId = 'dead-pod'
    await redis.set(resourceKey(orphanId), deadMemberId)

    const res = await coordinator.inject({
      method: 'POST',
      url: `/resources/${orphanId}/heartbeat`
    })
    strictEqual(res.statusCode, 204)

    const newMember = await redis.get(resourceKey(orphanId))
    ok(newMember === memberId1 || newMember === memberId2, 'should reassign to a live pod')
  })

  await t.test('lookupAndDeregister: deletes the resource and the registry mapping', async () => {
    const spawnRes = await coordinator.inject({ method: 'POST', url: '/resources' })
    const id = (spawnRes.json() as any).resourceId

    const delRes = await coordinator.inject({
      method: 'DELETE',
      url: `/resources/${id}`
    })
    strictEqual(delRes.statusCode, 204)

    const mapping = await redis.get(resourceKey(id))
    strictEqual(mapping, null)
  })

  await t.test('lookupAndDeregister: fast-paths when the pod is dead', async () => {
    const orphanId = `orphan-del-${randomBytes(3).toString('hex')}`
    const deadMemberId = 'dead-pod'
    await redis.set(resourceKey(orphanId), deadMemberId)

    const totalSpawnsBefore = pod1.resources.size + pod2.resources.size
    const res = await coordinator.inject({
      method: 'DELETE',
      url: `/resources/${orphanId}`
    })
    strictEqual(res.statusCode, 204)

    const mapping = await redis.get(resourceKey(orphanId))
    strictEqual(mapping, null, 'mapping should be removed')

    const totalSpawnsAfter = pod1.resources.size + pod2.resources.size
    strictEqual(totalSpawnsAfter, totalSpawnsBefore, 'no proxy call should reach a live pod')
  })

  await t.test('lookupAndDeregister: returns 404 when resource is unknown', async () => {
    const res = await coordinator.inject({ method: 'DELETE', url: '/resources/never-existed' })
    strictEqual(res.statusCode, 404)
  })
})
