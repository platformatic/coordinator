import { strictEqual, ok } from 'node:assert'
import test from 'node:test'
import { setTimeout as sleep } from 'node:timers/promises'
import { TTLCache } from '../src/cache.ts'

test('TTLCache returns undefined for missing keys', () => {
  const c = new TTLCache<string, number>({ ttl: 1000 })
  strictEqual(c.get('missing'), undefined)
})

test('TTLCache returns set values within TTL', () => {
  const c = new TTLCache<string, number>({ ttl: 1000 })
  c.set('a', 1)
  strictEqual(c.get('a'), 1)
})

test('TTLCache expires entries past TTL', async () => {
  const c = new TTLCache<string, number>({ ttl: 50 })
  c.set('a', 1)
  await sleep(80)
  strictEqual(c.get('a'), undefined)
})

test('TTLCache evicts oldest when max is reached', () => {
  const c = new TTLCache<string, number>({ ttl: 60_000, max: 2 })
  c.set('a', 1)
  c.set('b', 2)
  c.set('c', 3)
  strictEqual(c.get('a'), undefined, 'oldest evicted')
  strictEqual(c.get('b'), 2)
  strictEqual(c.get('c'), 3)
})

test('TTLCache get refreshes insertion order', () => {
  const c = new TTLCache<string, number>({ ttl: 60_000, max: 2 })
  c.set('a', 1)
  c.set('b', 2)
  // Read 'a' so it becomes the most-recently-used.
  ok(c.get('a'))
  c.set('c', 3)
  strictEqual(c.get('b'), undefined, 'b was evicted, not a')
  strictEqual(c.get('a'), 1)
  strictEqual(c.get('c'), 3)
})

test('TTLCache delete removes entries', () => {
  const c = new TTLCache<string, number>({ ttl: 1000 })
  c.set('a', 1)
  c.delete('a')
  strictEqual(c.get('a'), undefined)
})

test('TTLCache clear empties the cache', () => {
  const c = new TTLCache<string, number>({ ttl: 1000 })
  c.set('a', 1)
  c.set('b', 2)
  c.clear()
  strictEqual(c.get('a'), undefined)
  strictEqual(c.get('b'), undefined)
})
