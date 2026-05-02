import { strictEqual, ok } from 'node:assert'
import test from 'node:test'
import { RoundRobinStrategy, LeastLoadedStrategy, RandomStrategy, createStrategy } from '../src/strategies.ts'
import type { MemberWithLoad } from '../src/strategies.ts'

const members: MemberWithLoad[] = [
  { memberId: 'pod-1', address: 'http://localhost:3001', resourceCount: 5 },
  { memberId: 'pod-2', address: 'http://localhost:3002', resourceCount: 2 },
  { memberId: 'pod-3', address: 'http://localhost:3003', resourceCount: 8 }
]

test('RoundRobinStrategy - cycles through members', () => {
  const strategy = new RoundRobinStrategy()
  const first = strategy.pick(members)
  const second = strategy.pick(members)
  const third = strategy.pick(members)
  const fourth = strategy.pick(members)

  ok(first)
  ok(second)
  ok(third)
  ok(fourth)

  strictEqual(first.memberId, 'pod-1')
  strictEqual(second.memberId, 'pod-2')
  strictEqual(third.memberId, 'pod-3')
  strictEqual(fourth.memberId, 'pod-1')
})

test('RoundRobinStrategy - returns null for empty list', () => {
  const strategy = new RoundRobinStrategy()
  strictEqual(strategy.pick([]), null)
})

test('LeastLoadedStrategy - picks member with fewest resources', () => {
  const strategy = new LeastLoadedStrategy()
  const picked = strategy.pick(members)
  ok(picked)
  strictEqual(picked.memberId, 'pod-2')
  strictEqual(picked.resourceCount, 2)
})

test('LeastLoadedStrategy - breaks ties with round-robin', () => {
  const strategy = new LeastLoadedStrategy()
  const tiedMembers: MemberWithLoad[] = [
    { memberId: 'pod-a', address: 'http://a', resourceCount: 3 },
    { memberId: 'pod-b', address: 'http://b', resourceCount: 3 },
    { memberId: 'pod-c', address: 'http://c', resourceCount: 5 }
  ]

  const first = strategy.pick(tiedMembers)
  const second = strategy.pick(tiedMembers)
  const third = strategy.pick(tiedMembers)

  ok(first)
  ok(second)
  ok(third)

  strictEqual(first.memberId, 'pod-a')
  strictEqual(second.memberId, 'pod-b')
  strictEqual(third.memberId, 'pod-a')
})

test('LeastLoadedStrategy - returns null for empty list', () => {
  const strategy = new LeastLoadedStrategy()
  strictEqual(strategy.pick([]), null)
})

test('RandomStrategy - returns a member from the list', () => {
  const strategy = new RandomStrategy()
  const ids = new Set<string>()
  for (let i = 0; i < 50; i++) {
    const picked = strategy.pick(members)
    ok(picked)
    ids.add(picked.memberId)
  }
  ok(ids.size > 1, 'random should pick different members')
})

test('RandomStrategy - returns null for empty list', () => {
  const strategy = new RandomStrategy()
  strictEqual(strategy.pick([]), null)
})

test('createStrategy - returns correct strategy types', () => {
  ok(createStrategy('round-robin') instanceof RoundRobinStrategy)
  ok(createStrategy('least-loaded') instanceof LeastLoadedStrategy)
  ok(createStrategy('random') instanceof RandomStrategy)
  ok(createStrategy('unknown') instanceof RoundRobinStrategy)
})
