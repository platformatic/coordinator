# @platformatic/coordinator

Multi-pod destination routing for stateful tiers. Valkey-backed registry, pod-side `Member` class, allocation strategies, lock routing, TTL cache, and optional Fastify helpers.

See [`coordinator-pattern.md`](./coordinator-pattern.md) for the architecture this library implements: caller / coordinator / resource pod, failover, fan-out, transactions and locks.

## What it solves

You have N pods that hold stateful resources (PostgreSQL connection pools, agent processes, sandboxes, simulations). Requests carry a routing key -- a "destination" or "instance" id. You need every request for that destination to land on a pod that owns it. Pods die; surviving pods should take over. Under sustained load, a single destination may need to live on more than one pod.

This library handles all of that:

- Destination → pod set, persisted in Valkey
- Pod self-registration + heartbeat, with `total_connections` published as a metric
- Atomic first-touch claim, atomic failover (SREM dead + SADD fresh)
- Pluggable allocation strategies (round-robin, least-loaded by total connections, random)
- Lock routing for transaction-bound calls (lockId → pod, resolved via Valkey)
- A short-lived local cache for the hot resolution path
- Fastify helpers for HTTP coordinators

## Install

```sh
npm install @platformatic/coordinator
```

For the Fastify helpers, also:

```sh
npm install @fastify/reply-from
```

Peer dependency: `fastify >= 5` when using the Fastify helpers.

## Valkey layout

With `keyPrefix: 'myservice'`:

| Key | Type | Owner | Purpose |
|---|---|---|---|
| `myservice:members` | set | pod | the set of memberIds known to be live |
| `myservice:member:<memberId>` | hash with `address`, `load` | pod | live pod registration and load metric, TTL refreshed by heartbeat |
| `myservice:destination:<id>` | set of memberIds | coordinator + pod | pods currently serving this destination |
| `myservice:lock:<lockId>` | hash with `podId`, `destinationId`, metadata | pod | lockId routing for transaction-bound calls |

## Pod side: `Member`

The pod-side class owns its own iovalkey connection and writes the keys the pod is responsible for.

```ts
import { Member } from '@platformatic/coordinator'

const member = new Member({
  redis: 'redis://valkey:6379',
  memberId: 'pod-1',
  address: 'http://pod-1.local:3000',
  keyPrefix: 'myservice',
  ttl: 30,                          // seconds; default 30
  getLoad: () => pool.openCount()   // optional; default () => 0
})

await member.register()                                 // SADD + HSET + EXPIRE
const heartbeat = setInterval(() => member.heartbeat(), 10_000)  // HSET + EXPIRE
heartbeat.unref()

// When this pod fans itself in to a destination:
await member.addToDestination(destId)
await member.removeFromDestination(destId)

// When this pod mints / releases a transaction lock:
await member.registerLock(lockId, destId, { isolationLevel: 'serializable' })
await member.unregisterLock(lockId)

// Peer query for fan-out picks (returns live pods with their load):
const peers = await member.listPeerLoad()

// Graceful shutdown:
clearInterval(heartbeat)
await member.deregister()
await member.close()
```

## Coordinator side: `Registry`

```ts
import { Registry } from '@platformatic/coordinator'

const registry = new Registry({
  redis: 'redis://valkey:6379',
  keyPrefix: 'myservice',
  strategy: 'least-loaded',
  cache: { ttl: 5000, max: 10_000 }  // default; pass `false` to disable
})

// Hot path: resolve a destination, pick one pod, return its address.
const resolved = await registry.resolveDestination(destId, {
  claimOnMiss: true,      // SADD a fresh pod if the destination's set is empty
  reassignOrphans: true   // SREM dead + SADD fresh if every pod in the set is dead
})
if (resolved) {
  // { address, memberId, reassigned }
}

// Lock-bound call: route by lockId, not destination.
const lockRouting = await registry.resolveLock(lockId)
if (lockRouting) {
  // { address, memberId }
}

// Other primitives:
await registry.listLiveMembers()                  // [{ memberId, address, load }, ...]
await registry.pickMember({ destinationId: destId })
await registry.addPodToDestination(destId, memberId)
await registry.hasBinding(destId)
await registry.deregisterDestination(destId)
await registry.close()
```

## Resolution and failover

`resolveDestination` reads the destination's pod set, filters by liveness, and applies the allocation strategy. The four cases:

| Set state | `claimOnMiss` | `reassignOrphans` | Result |
|---|---|---|---|
| Empty | false | -- | `null` (404 territory) |
| Empty | true | -- | Pick a live pod, `SADD` it, return |
| Has live pods (possibly with dead too) | -- | -- | Pick one of the live pods; dead ones cleaned up in background |
| All dead, non-empty | -- | false | `null` |
| All dead, non-empty | -- | true | Pick fresh, `SREM` dead + `SADD` fresh, return with `reassigned: true` |

All writes use `SADD` / `SREM` (atomic). Concurrent first-touch by two coordinators can produce a destination with two pods from the start. That's a valid steady state, not a corrupted one.

## Allocation strategies

Pluggable. Built-in: `round-robin` (default), `least-loaded`, `random`. Custom strategies implement:

```ts
interface AllocationStrategy {
  pick (candidates: MemberInfo[], ctx: { destinationId?: string }): MemberInfo | null
}
```

`candidates` is the pool to choose from -- the full live set on first-touch / failover, or the live members of a destination's pod set on the hot path for fanned-out destinations. `ctx.destinationId` is the destination, so custom strategies can branch on it (for example, pin "dedicated" tenants to a designated subset of pods and round-robin "shared" tenants across the rest).

Built-in least-loaded reads `load` from each candidate's member record (`HGET` pipeline). It runs at first touch for single-pod destinations and on every request for fanned-out destinations.

## TTL cache

`resolveDestination` checks a local LRU+TTL cache before reading Valkey. Default 5 s TTL, 10 000 entries. Configure with `cache: { ttl, max }` or disable with `cache: false`. Writes through the registry (`addPodToDestination`, `deregisterDestination`) evict the affected key. Each replica has its own cache.

## Fastify helpers

For HTTP-based coordinators, three helpers wrap the common patterns. Each emits a tagged result via an optional `onResult` callback so presets can hook their own metric counters.

### `lookupAndProxy`

```ts
app.post('/destinations/:id/work', lookupAndProxy(registry, {
  destinationFrom: req => req.params.id,
  reassignOrphans: true,
  onResult: result => metrics.inc({ type: 'work', result }) // 'hit' | 'orphan_reassigned' | 'not_found'
}))
```

Resolves the destination, proxies via `reply.from`, returns 404 if the destination has no live pod.

### `pickAndRegister`

```ts
app.post('/destinations', pickAndRegister(registry, {
  registerIdFrom: res => res.id
}))
```

Picks a pod, proxies the create request, and `SADD`s the returned id to the destination set only on a 2xx upstream response. Returns 503 if there are no live pods.

### `lookupAndDeregister`

```ts
app.delete('/destinations/:id', lookupAndDeregister(registry, {
  destinationFrom: req => req.params.id
}))
```

Resolves, proxies the delete; on `expectedStatus` (204 by default), `DEL`s the destination set. If the destination has only dead pods, skips the proxy and just deletes the set ("deregistered_dead_pod").

All three helpers go through `@fastify/reply-from`, which the host application must register once before any helper-backed route is mounted.

## Testing

Tests use Redis on `127.0.0.1:6390`. A `docker-compose.yml` is included.

```sh
pnpm run test:redis:up
pnpm test
pnpm run test:redis:down
```

The URL is read from `REDIS_URL` (default `redis://127.0.0.1:6390`). Tests isolate keys with a random prefix and clean up after themselves.

## License

Apache-2.0
