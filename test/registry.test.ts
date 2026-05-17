import { strictEqual, ok, deepStrictEqual } from 'node:assert'
import { randomBytes } from 'node:crypto'
import test from 'node:test'
import { Redis } from 'iovalkey'
import { Registry } from '../src/registry.ts'
import { REDIS_URL } from './redis-url.ts'

const PREFIX = `test-${randomBytes(4).toString('hex')}`

const membersKey = (): string => `${PREFIX}:members`
const memberKey = (id: string): string => `${PREFIX}:member:${id}`
const destinationKey = (id: string): string => `${PREFIX}:destination:${id}`
const lockKey = (id: string): string => `${PREFIX}:lock:${id}`

async function makeLivePod (redis: Redis, memberId: string, address: string, load = 0): Promise<void> {
  await redis.sadd(membersKey(), memberId)
  await redis.hset(memberKey(memberId), { address, load: String(load) })
  await redis.expire(memberKey(memberId), 30)
}

test('Registry', async (t) => {
  const sharedRedis = new Redis(REDIS_URL)
  const registry = new Registry({ redis: REDIS_URL, keyPrefix: PREFIX, cache: false })

  const m1 = 'member-1'
  const m1Address = 'http://localhost:3001'
  const m2 = 'member-2'
  const m2Address = 'http://localhost:3002'

  t.after(async () => {
    const stream = sharedRedis.scanStream({ match: `${PREFIX}:*`, count: 100 })
    for await (const keys of stream) {
      if (keys.length > 0) await sharedRedis.del(...keys)
    }
    await registry.close()
    await sharedRedis.quit()
  })

  await t.test('listLiveMembers returns empty when no members registered', async () => {
    const live = await registry.listLiveMembers()
    deepStrictEqual(live, [])
  })

  await t.test('listLiveMembers returns hash-based pods with load', async () => {
    await makeLivePod(sharedRedis, m1, m1Address, 3)
    await makeLivePod(sharedRedis, m2, m2Address, 7)

    const live = await registry.listLiveMembers()
    strictEqual(live.length, 2)

    const a = live.find(m => m.memberId === m1)
    const b = live.find(m => m.memberId === m2)
    ok(a); ok(b)
    strictEqual(a.address, m1Address)
    strictEqual(a.load, 3)
    strictEqual(b.load, 7)
  })

  await t.test('listLiveMembers skips members whose hash has expired', async () => {
    await sharedRedis.del(memberKey(m2))
    const live = await registry.listLiveMembers()
    strictEqual(live.length, 1)
    strictEqual(live[0].memberId, m1)
    await makeLivePod(sharedRedis, m2, m2Address, 7)
  })

  await t.test('resolveDestination returns null for unknown instance without claimOnMiss', async () => {
    strictEqual(await registry.resolveDestination('unknown'), null)
  })

  await t.test('resolveDestination with claimOnMiss SADDs a fresh pod and returns it', async () => {
    const result = await registry.resolveDestination('inst-claim', { claimOnMiss: true })
    ok(result)
    ok(result.address === m1Address || result.address === m2Address)
    strictEqual(result.reassigned, false)

    const set = await sharedRedis.smembers(destinationKey('inst-claim'))
    deepStrictEqual(set, [result.memberId])

    await sharedRedis.del(destinationKey('inst-claim'))
  })

  await t.test('resolveDestination returns address from existing single-pod set', async () => {
    await sharedRedis.sadd(destinationKey('inst-existing'), m1)
    const result = await registry.resolveDestination('inst-existing')
    ok(result)
    strictEqual(result.address, m1Address)
    strictEqual(result.memberId, m1)
    strictEqual(result.reassigned, false)
    await sharedRedis.del(destinationKey('inst-existing'))
  })

  await t.test('resolveDestination returns null when set is non-empty but all pods are dead and reassignOrphans=false', async () => {
    await sharedRedis.sadd(destinationKey('inst-orphan'), 'dead-pod')
    const result = await registry.resolveDestination('inst-orphan')
    strictEqual(result, null)
    // The dead binding is preserved (caller can choose to clean it up explicitly).
    const set = await sharedRedis.smembers(destinationKey('inst-orphan'))
    deepStrictEqual(set, ['dead-pod'])
    await sharedRedis.del(destinationKey('inst-orphan'))
  })

  await t.test('resolveDestination reassigns orphan with reassignOrphans=true', async () => {
    await sharedRedis.sadd(destinationKey('inst-reassign'), 'dead-pod')
    const result = await registry.resolveDestination('inst-reassign', { reassignOrphans: true })
    ok(result)
    strictEqual(result.reassigned, true)
    ok(result.address === m1Address || result.address === m2Address)

    const set = await sharedRedis.smembers(destinationKey('inst-reassign'))
    strictEqual(set.length, 1)
    ok(set[0] === m1 || set[0] === m2)
    ok(!set.includes('dead-pod'))
    await sharedRedis.del(destinationKey('inst-reassign'))
  })

  await t.test('resolveDestination with multi-pod set picks one live pod, cleans dead members in background', async () => {
    await sharedRedis.sadd(destinationKey('inst-multi'), m1, 'dead-pod', m2)
    const result = await registry.resolveDestination('inst-multi')
    ok(result)
    ok(result.memberId === m1 || result.memberId === m2)
    strictEqual(result.reassigned, false)

    // Give the background SREM a moment.
    await new Promise<void>(resolve => setTimeout(resolve, 30))
    const set = await sharedRedis.smembers(destinationKey('inst-multi'))
    ok(!set.includes('dead-pod'), 'dead member is removed eventually')
    await sharedRedis.del(destinationKey('inst-multi'))
  })

  await t.test('resolveDestination returns null when reassignOrphans=true but no live pods', async () => {
    await sharedRedis.del(memberKey(m1), memberKey(m2))
    await sharedRedis.sadd(destinationKey('inst-none'), 'dead-pod')

    const result = await registry.resolveDestination('inst-none', { reassignOrphans: true })
    strictEqual(result, null)

    await sharedRedis.del(destinationKey('inst-none'))
    await makeLivePod(sharedRedis, m1, m1Address, 3)
    await makeLivePod(sharedRedis, m2, m2Address, 7)
  })

  await t.test('addPodToDestination SADDs and invalidates cache', async () => {
    await registry.addPodToDestination('inst-add', m1)
    const set = await sharedRedis.smembers(destinationKey('inst-add'))
    deepStrictEqual(set, [m1])
    await sharedRedis.del(destinationKey('inst-add'))
  })

  await t.test('hasBinding returns true for a non-empty set, false otherwise', async () => {
    strictEqual(await registry.hasBinding('inst-empty'), false)
    await sharedRedis.sadd(destinationKey('inst-empty'), m1)
    strictEqual(await registry.hasBinding('inst-empty'), true)
    await sharedRedis.del(destinationKey('inst-empty'))
  })

  await t.test('deregisterDestination DELs the destination set', async () => {
    await sharedRedis.sadd(destinationKey('inst-del'), m1, m2)
    await registry.deregisterDestination('inst-del')
    const exists = await sharedRedis.exists(destinationKey('inst-del'))
    strictEqual(exists, 0)
  })

  await t.test('resolveLock returns null for unknown lockId', async () => {
    strictEqual(await registry.resolveLock('missing'), null)
  })

  await t.test('resolveLock returns the owning pod address', async () => {
    await sharedRedis.hset(lockKey('lock-x'), { podId: m1, destinationId: 'dest-1' })
    const result = await registry.resolveLock('lock-x')
    ok(result)
    strictEqual(result.memberId, m1)
    strictEqual(result.address, m1Address)
    await sharedRedis.del(lockKey('lock-x'))
  })

  await t.test('resolveLock returns null when the owning pod is dead', async () => {
    await sharedRedis.hset(lockKey('lock-dead'), { podId: 'dead-pod', destinationId: 'dest-1' })
    strictEqual(await registry.resolveLock('lock-dead'), null)
    await sharedRedis.del(lockKey('lock-dead'))
  })

  await t.test('pickMember round-robins across live pods', async () => {
    const first = await registry.pickMember({ destinationId: 'pick-test' })
    const second = await registry.pickMember({ destinationId: 'pick-test' })
    ok(first); ok(second)
    ok(first.memberId !== second.memberId, 'round-robin should cycle')
  })

  await t.test('keyPrefix isolates two registries pointed at the same Redis', async () => {
    const prefixA = `${PREFIX}-isoA-${randomBytes(2).toString('hex')}`
    const prefixB = `${PREFIX}-isoB-${randomBytes(2).toString('hex')}`
    const a = new Registry({ redis: REDIS_URL, keyPrefix: prefixA, cache: false })
    const b = new Registry({ redis: REDIS_URL, keyPrefix: prefixB, cache: false })

    try {
      await sharedRedis.sadd(`${prefixA}:members`, 'pod-x')
      await sharedRedis.hset(`${prefixA}:member:pod-x`, { address: 'http://x', load: '0' })
      await sharedRedis.expire(`${prefixA}:member:pod-x`, 30)

      strictEqual((await a.listLiveMembers()).length, 1)
      strictEqual((await b.listLiveMembers()).length, 0)
    } finally {
      await a.close()
      await b.close()
    }
  })

  await t.test('cache: resolveDestination hits the cache on the second call', async () => {
    const cached = new Registry({ redis: REDIS_URL, keyPrefix: PREFIX, cache: { ttl: 60_000 } })
    try {
      await sharedRedis.sadd(destinationKey('inst-cache'), m1)
      const first = await cached.resolveDestination('inst-cache')
      ok(first)

      // Mutate Valkey behind the cache; we should still see the cached value.
      await sharedRedis.del(destinationKey('inst-cache'))
      const second = await cached.resolveDestination('inst-cache')
      ok(second, 'cache returned the previously-resolved value')
      strictEqual(second.address, first.address)
    } finally {
      await cached.close()
    }
  })

  await t.test('cache is invalidated by deregisterDestination', async () => {
    const cached = new Registry({ redis: REDIS_URL, keyPrefix: PREFIX, cache: { ttl: 60_000 } })
    try {
      await sharedRedis.sadd(destinationKey('inst-inv'), m1)
      const first = await cached.resolveDestination('inst-inv')
      ok(first)

      await cached.deregisterDestination('inst-inv')
      const second = await cached.resolveDestination('inst-inv')
      strictEqual(second, null, 'cache was invalidated, lookup re-reads Valkey')
    } finally {
      await cached.close()
    }
  })

  await t.test('close quits the registry-owned Redis connection', async () => {
    const owned = new Registry({ redis: REDIS_URL, cache: false })
    await owned.close()
  })
})
