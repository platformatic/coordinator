import { strictEqual, ok, deepStrictEqual } from 'node:assert'
import { randomBytes } from 'node:crypto'
import test from 'node:test'
import { Redis } from 'iovalkey'
import { Registry } from '../src/registry.ts'
import { REDIS_URL } from './redis-url.ts'

const PREFIX = `test-${randomBytes(4).toString('hex')}`

function membersKey (): string {
  return `${PREFIX}:members`
}

function memberKey (memberId: string): string {
  return `${PREFIX}:member:${memberId}`
}

function resourceKey (resourceId: string): string {
  return `${PREFIX}:resource:${resourceId}`
}

function loadKey (memberId: string): string {
  return `${PREFIX}:member:${memberId}:resources`
}

test('Registry', async (t) => {
  const sharedRedis = new Redis(REDIS_URL)
  const registry = new Registry({ redis: REDIS_URL, keyPrefix: PREFIX })

  const member1Id = 'member-1'
  const member1Address = 'http://localhost:3001'
  const member2Id = 'member-2'
  const member2Address = 'http://localhost:3002'

  t.after(async () => {
    const stream = sharedRedis.scanStream({ match: `${PREFIX}:*`, count: 100 })
    for await (const keys of stream) {
      if (keys.length > 0) await sharedRedis.del(...keys)
    }
    await registry.close()
    await sharedRedis.quit()
  })

  await t.test('listMembers returns empty array when no members', async () => {
    const members = await registry.listMembers()
    deepStrictEqual(members, [])
  })

  await t.test('listMembers returns registered members', async () => {
    await sharedRedis.sadd(membersKey(), member1Id)
    await sharedRedis.set(memberKey(member1Id), member1Address, 'EX', 30)
    await sharedRedis.sadd(membersKey(), member2Id)
    await sharedRedis.set(memberKey(member2Id), member2Address, 'EX', 30)

    const members = await registry.listMembers()
    strictEqual(members.length, 2)

    const m1 = members.find(m => m.memberId === member1Id)
    ok(m1)
    strictEqual(m1.address, member1Address)

    const m2 = members.find(m => m.memberId === member2Id)
    ok(m2)
    strictEqual(m2.address, member2Address)
  })

  await t.test('listMembers skips members with expired keys', async () => {
    await sharedRedis.del(memberKey(member2Id))

    const members = await registry.listMembers()
    strictEqual(members.length, 1)
    strictEqual(members[0].memberId, member1Id)

    await sharedRedis.set(memberKey(member2Id), member2Address, 'EX', 30)
  })

  await t.test('lookupResource returns pod address via two-step lookup', async () => {
    const resourceId = 'res-1'
    await sharedRedis.set(resourceKey(resourceId), member1Id)

    const address = await registry.lookupResource(resourceId)
    strictEqual(address, member1Address)
  })

  await t.test('lookupResource returns null for unknown resource', async () => {
    const address = await registry.lookupResource('nonexistent')
    strictEqual(address, null)
  })

  await t.test('registerResource sets resource mapping', async () => {
    const resourceId = 'res-2'
    await registry.registerResource(resourceId, member2Id)

    const value = await sharedRedis.get(resourceKey(resourceId))
    strictEqual(value, member2Id)
  })

  await t.test('deregisterResource removes resource mapping', async () => {
    const resourceId = 'res-2'
    await registry.deregisterResource(resourceId)

    const value = await sharedRedis.get(resourceKey(resourceId))
    strictEqual(value, null)
  })

  await t.test('pickMember round-robins across members', async () => {
    const first = await registry.pickMember()
    ok(first)
    const second = await registry.pickMember()
    ok(second)
    ok(first.memberId !== second.memberId, 'round-robin should cycle through members')
  })

  await t.test('lookupResourceMemberId returns memberId for registered resource', async () => {
    const resourceId = 'res-1'
    const memberId = await registry.lookupResourceMemberId(resourceId)
    strictEqual(memberId, member1Id)
  })

  await t.test('lookupResourceMemberId returns null for unknown resource', async () => {
    const memberId = await registry.lookupResourceMemberId('nonexistent')
    strictEqual(memberId, null)
  })

  await t.test('resolveResource returns address for live resource', async () => {
    const result = await registry.resolveResource('res-1')
    ok(result)
    strictEqual(result.address, member1Address)
    strictEqual(result.reassigned, false)
  })

  await t.test('resolveResource returns null for completely unknown resource', async () => {
    const result = await registry.resolveResource('nonexistent')
    strictEqual(result, null)
  })

  await t.test('resolveResource returns address: null when pod is dead and reassignOrphans is false', async () => {
    const orphanId = 'res-orphan-no-reassign'
    const deadMemberId = 'dead-pod'
    await sharedRedis.set(resourceKey(orphanId), deadMemberId)

    const result = await registry.resolveResource(orphanId)
    ok(result)
    strictEqual(result.address, null)
    strictEqual(result.reassigned, false)

    // Mapping must still exist
    const mapping = await sharedRedis.get(resourceKey(orphanId))
    strictEqual(mapping, deadMemberId)

    await sharedRedis.del(resourceKey(orphanId))
  })

  await t.test('listMembersWithLoad returns resource counts', async () => {
    await sharedRedis.set(loadKey(member1Id), '3', 'EX', 30)
    await sharedRedis.set(loadKey(member2Id), '7', 'EX', 30)

    const members = await registry.listMembersWithLoad()
    strictEqual(members.length, 2)

    const m1 = members.find(m => m.memberId === member1Id)
    ok(m1)
    strictEqual(m1.resourceCount, 3)

    const m2 = members.find(m => m.memberId === member2Id)
    ok(m2)
    strictEqual(m2.resourceCount, 7)

    await sharedRedis.del(loadKey(member1Id), loadKey(member2Id))
  })

  await t.test('listMembersWithLoad defaults to 0 for missing count keys', async () => {
    const members = await registry.listMembersWithLoad()
    for (const member of members) {
      strictEqual(member.resourceCount, 0)
    }
  })

  await t.test('resolveResource detects orphan and reassigns when reassignOrphans is true', async () => {
    const orphanId = 'res-orphan-reassign'
    const deadMemberId = 'dead-pod'

    await sharedRedis.set(resourceKey(orphanId), deadMemberId)

    const result = await registry.resolveResource(orphanId, { reassignOrphans: true })
    ok(result)
    strictEqual(result.reassigned, true)
    ok(result.address === member1Address || result.address === member2Address)

    const newMemberId = await sharedRedis.get(resourceKey(orphanId))
    ok(newMemberId === member1Id || newMemberId === member2Id)

    await sharedRedis.del(resourceKey(orphanId))
  })

  await t.test('resolveResource returns address: null when reassignOrphans is true but no live pods', async () => {
    const orphanId = 'res-orphan-no-pods'
    const deadMemberId = 'dead-pod'

    // Temporarily remove all live pods
    await sharedRedis.del(memberKey(member1Id), memberKey(member2Id))
    await sharedRedis.set(resourceKey(orphanId), deadMemberId)

    const result = await registry.resolveResource(orphanId, { reassignOrphans: true })
    ok(result)
    strictEqual(result.address, null)
    strictEqual(result.reassigned, false)

    await sharedRedis.del(resourceKey(orphanId))
    await sharedRedis.set(memberKey(member1Id), member1Address, 'EX', 30)
    await sharedRedis.set(memberKey(member2Id), member2Address, 'EX', 30)
  })

  await t.test('pickMember returns null when no members available', async () => {
    const isolated = `${PREFIX}-empty-${randomBytes(2).toString('hex')}`
    const empty = new Registry({ redis: REDIS_URL, keyPrefix: isolated })
    try {
      const result = await empty.pickMember()
      strictEqual(result, null)
    } finally {
      await empty.close()
    }
  })

  await t.test('keyPrefix isolates two registries pointed at the same Redis', async () => {
    const prefixA = `${PREFIX}-isoA-${randomBytes(2).toString('hex')}`
    const prefixB = `${PREFIX}-isoB-${randomBytes(2).toString('hex')}`
    const a = new Registry({ redis: REDIS_URL, keyPrefix: prefixA })
    const b = new Registry({ redis: REDIS_URL, keyPrefix: prefixB })

    try {
      await sharedRedis.sadd(`${prefixA}:members`, 'pod-x')
      await sharedRedis.set(`${prefixA}:member:pod-x`, 'http://x', 'EX', 30)

      const aMembers = await a.listMembers()
      const bMembers = await b.listMembers()

      strictEqual(aMembers.length, 1)
      strictEqual(bMembers.length, 0)
    } finally {
      await a.close()
      await b.close()
    }
  })

  await t.test('close quits the registry-owned Redis connection', async () => {
    const owned = new Registry({ redis: REDIS_URL })
    await owned.close()
    // Should not hang or throw.
  })
})
