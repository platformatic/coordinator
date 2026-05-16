import { strictEqual, ok, deepStrictEqual } from 'node:assert'
import { randomBytes } from 'node:crypto'
import { setTimeout as sleep } from 'node:timers/promises'
import test from 'node:test'
import { Redis } from 'iovalkey'
import { Member } from '../src/member.ts'
import { REDIS_URL } from './redis-url.ts'

const PREFIX = `test-${randomBytes(4).toString('hex')}`

const membersKey = (): string => `${PREFIX}:members`
const memberKey = (id: string): string => `${PREFIX}:member:${id}`
const destinationKey = (id: string): string => `${PREFIX}:destination:${id}`
const lockKey = (id: string): string => `${PREFIX}:lock:${id}`

test('Member', async (t) => {
  const sharedRedis = new Redis(REDIS_URL)
  const memberId = 'member-1'
  const address = 'http://localhost:3001'

  let connections = 0
  const member = new Member({
    redis: REDIS_URL,
    memberId,
    address,
    keyPrefix: PREFIX,
    getLoad: () => connections
  })

  t.after(async () => {
    const stream = sharedRedis.scanStream({ match: `${PREFIX}:*`, count: 100 })
    for await (const keys of stream) {
      if (keys.length > 0) await sharedRedis.del(...keys)
    }
    await member.close()
    await sharedRedis.quit()
  })

  await t.test('register adds member to set and writes hash with TTL', async () => {
    connections = 7
    await member.register()

    const isMember = await sharedRedis.sismember(membersKey(), memberId)
    strictEqual(isMember, 1)

    const fields = await sharedRedis.hmget(memberKey(memberId), 'address', 'load')
    deepStrictEqual(fields, [address, '7'])

    const ttl = await sharedRedis.ttl(memberKey(memberId))
    ok(ttl > 0 && ttl <= 30, `TTL should be between 1 and 30, got ${ttl}`)
  })

  await t.test('heartbeat updates load and refreshes TTL', async () => {
    connections = 7
    await member.register()

    connections = 42
    await sleep(1100)
    const ttlBefore = await sharedRedis.ttl(memberKey(memberId))

    await member.heartbeat()

    const updated = await sharedRedis.hget(memberKey(memberId), 'load')
    strictEqual(updated, '42')

    const ttlAfter = await sharedRedis.ttl(memberKey(memberId))
    ok(ttlAfter >= ttlBefore, `TTL after (${ttlAfter}) >= before (${ttlBefore})`)
  })

  await t.test('deregister removes member from set and deletes hash', async () => {
    await member.register()
    await member.deregister()

    const isMember = await sharedRedis.sismember(membersKey(), memberId)
    strictEqual(isMember, 0)

    const exists = await sharedRedis.exists(memberKey(memberId))
    strictEqual(exists, 0)
  })

  await t.test('addToDestination SADDs self to the destination set', async () => {
    await member.addToDestination('dest-A')
    const members = await sharedRedis.smembers(destinationKey('dest-A'))
    deepStrictEqual(members.sort(), [memberId])
  })

  await t.test('removeFromDestination SREMs self', async () => {
    await member.addToDestination('dest-B')
    await member.removeFromDestination('dest-B')
    const members = await sharedRedis.smembers(destinationKey('dest-B'))
    deepStrictEqual(members, [])
  })

  await t.test('registerLock writes lock record with podId and destinationId', async () => {
    await member.registerLock('lock-1', 'dest-X', { isolationLevel: 'serializable' })
    const fields = await sharedRedis.hgetall(lockKey('lock-1'))
    strictEqual(fields.podId, memberId)
    strictEqual(fields.destinationId, 'dest-X')
    strictEqual(fields.isolationLevel, 'serializable')
  })

  await t.test('unregisterLock deletes the lock record', async () => {
    await member.registerLock('lock-2', 'dest-Y')
    await member.unregisterLock('lock-2')
    const exists = await sharedRedis.exists(lockKey('lock-2'))
    strictEqual(exists, 0)
  })

  await t.test('listPeerLoad returns live members with load', async () => {
    connections = 5
    await member.register()

    // Add a peer manually.
    await sharedRedis.sadd(membersKey(), 'peer-1')
    await sharedRedis.hset(memberKey('peer-1'), { address: 'http://peer:9000', load: '12' })
    await sharedRedis.expire(memberKey('peer-1'), 30)

    const peers = await member.listPeerLoad()
    strictEqual(peers.length, 2)

    const self = peers.find(p => p.memberId === memberId)
    const peer = peers.find(p => p.memberId === 'peer-1')
    ok(self); ok(peer)
    strictEqual(self.load, 5)
    strictEqual(peer.load, 12)
    strictEqual(peer.address, 'http://peer:9000')
  })

  await t.test('listPeerLoad skips members whose hash has expired', async () => {
    await sharedRedis.del(memberKey('peer-1'))
    const peers = await member.listPeerLoad()
    ok(peers.every(p => p.memberId !== 'peer-1'))
  })

  await t.test('custom ttl is respected', async () => {
    const m = new Member({ redis: REDIS_URL, memberId: 'm-ttl', address, keyPrefix: PREFIX, ttl: 5 })
    await m.register()
    const ttl = await sharedRedis.ttl(memberKey('m-ttl'))
    ok(ttl > 0 && ttl <= 5, `custom TTL should be <= 5, got ${ttl}`)
    await m.close()
  })

  await t.test('getLoad defaults to () => 0 when omitted', async () => {
    const m = new Member({ redis: REDIS_URL, memberId: 'm-default', address, keyPrefix: PREFIX })
    await m.register()
    const v = await sharedRedis.hget(memberKey('m-default'), 'load')
    strictEqual(v, '0')
    await m.close()
  })
})
