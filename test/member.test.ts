import { strictEqual, ok } from 'node:assert'
import { randomBytes } from 'node:crypto'
import { setTimeout as sleep } from 'node:timers/promises'
import test from 'node:test'
import { Redis } from 'iovalkey'
import { Member } from '../src/member.ts'
import { REDIS_URL } from './redis-url.ts'

const PREFIX = `test-${randomBytes(4).toString('hex')}`

function membersKey (): string {
  return `${PREFIX}:members`
}

function memberKey (memberId: string): string {
  return `${PREFIX}:member:${memberId}`
}

function instanceKey (instanceId: string): string {
  return `${PREFIX}:instance:${instanceId}`
}

function loadKey (memberId: string): string {
  return `${PREFIX}:member:${memberId}:instances`
}

test('Member', async (t) => {
  const sharedRedis = new Redis(REDIS_URL)
  const memberId = 'member-1'
  const address = 'http://localhost:3001'
  const member = new Member({ redis: REDIS_URL, memberId, address, keyPrefix: PREFIX })

  t.after(async () => {
    const stream = sharedRedis.scanStream({ match: `${PREFIX}:*`, count: 100 })
    for await (const keys of stream) {
      if (keys.length > 0) await sharedRedis.del(...keys)
    }
    await member.close()
    await sharedRedis.quit()
  })

  await t.test('register adds member to set and sets address with TTL', async () => {
    await member.register()

    const isMember = await sharedRedis.sismember(membersKey(), memberId)
    strictEqual(isMember, 1)

    const stored = await sharedRedis.get(memberKey(memberId))
    strictEqual(stored, address)

    const ttl = await sharedRedis.ttl(memberKey(memberId))
    ok(ttl > 0 && ttl <= 30, `TTL should be between 1 and 30, got ${ttl}`)
  })

  await t.test('register initializes load count to 0 with TTL', async () => {
    await member.register()
    const count = await sharedRedis.get(loadKey(memberId))
    strictEqual(count, '0')

    const ttl = await sharedRedis.ttl(loadKey(memberId))
    ok(ttl > 0 && ttl <= 30, `load TTL should be between 1 and 30, got ${ttl}`)
  })

  await t.test('deregister removes member and load key', async () => {
    await member.register()
    await member.deregister()

    const isMember = await sharedRedis.sismember(membersKey(), memberId)
    strictEqual(isMember, 0)

    const stored = await sharedRedis.get(memberKey(memberId))
    strictEqual(stored, null)

    const count = await sharedRedis.get(loadKey(memberId))
    strictEqual(count, null)
  })

  await t.test('heartbeat refreshes both TTLs', async () => {
    await member.register()

    await sleep(1100)
    const addressTtlBefore = await sharedRedis.ttl(memberKey(memberId))
    const loadTtlBefore = await sharedRedis.ttl(loadKey(memberId))

    await member.heartbeat()
    const addressTtlAfter = await sharedRedis.ttl(memberKey(memberId))
    const loadTtlAfter = await sharedRedis.ttl(loadKey(memberId))

    ok(addressTtlAfter >= addressTtlBefore, `address TTL after (${addressTtlAfter}) >= before (${addressTtlBefore})`)
    ok(loadTtlAfter >= loadTtlBefore, `load TTL after (${loadTtlAfter}) >= before (${loadTtlBefore})`)
  })

  await t.test('registerInstance sets mapping and increments load', async () => {
    await member.register()
    const instanceId = 'inst-1'
    await member.registerInstance(instanceId)

    const stored = await sharedRedis.get(instanceKey(instanceId))
    strictEqual(stored, memberId)

    const count = await sharedRedis.get(loadKey(memberId))
    strictEqual(count, '1')
  })

  await t.test('deregisterInstance removes mapping and decrements load', async () => {
    await member.register()
    const instanceId = 'inst-2'
    await member.registerInstance(instanceId)
    const before = parseInt(await sharedRedis.get(loadKey(memberId)) ?? '0', 10)

    await member.deregisterInstance(instanceId)

    const stored = await sharedRedis.get(instanceKey(instanceId))
    strictEqual(stored, null)

    const after = parseInt(await sharedRedis.get(loadKey(memberId)) ?? '0', 10)
    strictEqual(after, before - 1)
  })

  await t.test('lookupInstance returns address via two-step lookup', async () => {
    await member.register()
    const instanceId = 'inst-3'
    await member.registerInstance(instanceId)

    const resolved = await member.lookupInstance(instanceId)
    strictEqual(resolved, address)
  })

  await t.test('lookupInstance returns null for unknown instance', async () => {
    const resolved = await member.lookupInstance('nonexistent')
    strictEqual(resolved, null)
  })

  await t.test('custom ttl is respected', async () => {
    const m = new Member({ redis: REDIS_URL, memberId: 'm-ttl', address, keyPrefix: PREFIX, ttl: 5 })
    await m.register()
    const ttl = await sharedRedis.ttl(memberKey('m-ttl'))
    ok(ttl > 0 && ttl <= 5, `custom TTL should be <= 5, got ${ttl}`)
    await m.close()
  })
})
