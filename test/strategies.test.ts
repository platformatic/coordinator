import { strictEqual, ok } from 'node:assert'
import test from 'node:test'
import { RoundRobinStrategy, LeastLoadedStrategy, RandomStrategy, createStrategy } from '../src/strategies.ts'
import type { MemberInfo } from '../src/strategies.ts'

const members: MemberInfo[] = [
  { memberId: 'pod-1', address: 'http://localhost:3001', load: 5 },
  { memberId: 'pod-2', address: 'http://localhost:3002', load: 2 },
  { memberId: 'pod-3', address: 'http://localhost:3003', load: 8 }
]

const ctx = { destinationId: 'inst-x' }

test('RoundRobinStrategy - cycles through candidates', () => {
  const strategy = new RoundRobinStrategy()
  const first = strategy.pick(members, ctx)
  const second = strategy.pick(members, ctx)
  const third = strategy.pick(members, ctx)
  const fourth = strategy.pick(members, ctx)

  ok(first); ok(second); ok(third); ok(fourth)
  strictEqual(first.memberId, 'pod-1')
  strictEqual(second.memberId, 'pod-2')
  strictEqual(third.memberId, 'pod-3')
  strictEqual(fourth.memberId, 'pod-1')
})

test('RoundRobinStrategy - returns null for empty list', () => {
  strictEqual(new RoundRobinStrategy().pick([], ctx), null)
})

test('LeastLoadedStrategy - picks candidate with fewest total connections', () => {
  const picked = new LeastLoadedStrategy().pick(members, ctx)
  ok(picked)
  strictEqual(picked.memberId, 'pod-2')
  strictEqual(picked.load, 2)
})

test('LeastLoadedStrategy - breaks ties with round-robin', () => {
  const strategy = new LeastLoadedStrategy()
  const tied: MemberInfo[] = [
    { memberId: 'pod-a', address: 'http://a', load: 3 },
    { memberId: 'pod-b', address: 'http://b', load: 3 },
    { memberId: 'pod-c', address: 'http://c', load: 5 }
  ]

  const first = strategy.pick(tied, ctx)
  const second = strategy.pick(tied, ctx)
  const third = strategy.pick(tied, ctx)

  ok(first); ok(second); ok(third)
  strictEqual(first.memberId, 'pod-a')
  strictEqual(second.memberId, 'pod-b')
  strictEqual(third.memberId, 'pod-a')
})

test('LeastLoadedStrategy - returns null for empty list', () => {
  strictEqual(new LeastLoadedStrategy().pick([], ctx), null)
})

test('RandomStrategy - returns a candidate from the list', () => {
  const strategy = new RandomStrategy()
  const ids = new Set<string>()
  for (let i = 0; i < 50; i++) {
    const picked = strategy.pick(members, ctx)
    ok(picked)
    ids.add(picked.memberId)
  }
  ok(ids.size > 1, 'random should pick different candidates')
})

test('RandomStrategy - returns null for empty list', () => {
  strictEqual(new RandomStrategy().pick([], ctx), null)
})

test('Custom strategy receives ctx with destinationId', () => {
  let seenDestinationId: string | undefined
  const strategy = {
    pick (candidates: MemberInfo[], pickCtx: { destinationId?: string }) {
      seenDestinationId = pickCtx.destinationId
      return candidates[0] ?? null
    }
  }
  strategy.pick(members, { destinationId: 'tenant-42' })
  strictEqual(seenDestinationId, 'tenant-42')
})

test('createStrategy - returns correct strategy types', () => {
  ok(createStrategy('round-robin') instanceof RoundRobinStrategy)
  ok(createStrategy('least-loaded') instanceof LeastLoadedStrategy)
  ok(createStrategy('random') instanceof RandomStrategy)
  ok(createStrategy('unknown') instanceof RoundRobinStrategy)
})
