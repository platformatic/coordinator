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

Peer dependency: `fastify >= 5` when using the Fastify plugin or helpers. `@fastify/reply-from` is a runtime dependency of this package — you don't need to install it separately.

## Quick start (Fastify plugin)

```ts
import Fastify from 'fastify'
import coordinatorPlugin from '@platformatic/coordinator'

const app = Fastify()

await app.register(coordinatorPlugin, {
  redis: 'redis://valkey:6379',
  keyPrefix: 'myservice',
  strategy: 'least-loaded'
})

app.get('/destinations/:id/work', app.coordinator.lookupAndProxy({
  destinationFrom: req => req.params.id,
  claimOnMiss: true,
  reassignOrphans: true
}))

app.post('/destinations', app.coordinator.pickAndRegister({
  registerIdFrom: res => res.id
}))

app.delete('/destinations/:id', app.coordinator.lookupAndDeregister({
  destinationFrom: req => req.params.id
}))

app.post('/transactions/:lockId/work', app.coordinator.lookupLockAndProxy({
  lockFrom: req => req.params.lockId
}))

await app.listen({ port: 3000 })
```

The plugin:

- Registers `@fastify/reply-from` (idempotently — skipped if already registered)
- Constructs a `Registry` from the passed options (or reuses one you provide via `registry`)
- Exposes both the registry and the route-handler-factory helpers on `app.coordinator`
- Closes the registry on `app.close()` (unless you brought your own)

Options:

```ts
interface CoordinatorPluginOptions {
  // Forwarded to new Registry(...) when no `registry` is supplied:
  redis?: string                                       // redis/valkey URL
  keyPrefix?: string                                   // default 'coordinator'
  strategy?: 'round-robin' | 'least-loaded' | 'random' | AllocationStrategy
  cache?: { ttl?: number, max?: number } | false
  requestTimeout?: number

  registry?: Registry                                  // reuse an existing Registry (plugin will not close it)
  decorateAs?: string                                  // default 'coordinator'
  replyFrom?: FastifyReplyFromOptions                  // forwarded to @fastify/reply-from
  registerReplyFrom?: boolean                          // default true; set false if you already registered reply-from
}
```

The legacy standalone helper imports (`import { lookupAndProxy } from '@platformatic/coordinator'`) still work and are documented below. They require manually registering `@fastify/reply-from` and constructing the `Registry`.

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

## Fastify helpers (standalone, advanced)

The helpers used internally by `app.coordinator.*` are also exported as standalone functions, for users who want to manage their own `Registry` and reply-from registration. Each emits a tagged result via an optional `onResult` callback so presets can hook their own metric counters.

Before mounting any helper-backed route you must register `@fastify/reply-from` (the `coordinatorPlugin` does this for you):

```ts
import Fastify from 'fastify'
import replyFrom from '@fastify/reply-from'
import { Registry, lookupAndProxy } from '@platformatic/coordinator'

const app = Fastify()
await app.register(replyFrom)
const registry = new Registry({ redis, keyPrefix })
app.get('/x/:id', lookupAndProxy(registry, { destinationFrom: r => r.params.id }))
```

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

All four helpers go through `@fastify/reply-from`. The `coordinatorPlugin` registers it automatically; if you use the standalone helpers, you must register it yourself.

### `lookupLockAndProxy`

```ts
app.post('/transactions/:lockId/work', lookupLockAndProxy(registry, {
  lockFrom: req => req.params.lockId
}))
```

Resolves the lockId to the pod that owns it (via `Registry.resolveLock`) and proxies through. 404s if the lockId is unknown.

## Testing

Unit tests need a Redis on `127.0.0.1:6390`. E2E tests also need a Postgres on `127.0.0.1:15432` (storage/storage/storage). Both are in the included `docker-compose.yml`.

```sh
pnpm run test:deps:up   # brings up redis + postgres
pnpm test               # unit tests
pnpm run test:e2e       # end-to-end tests (uses the storage-db example)
pnpm run test:deps:down
```

URLs are read from `REDIS_URL` (default `redis://127.0.0.1:6390`) and `PG_URL` (default `postgresql://storage:storage@127.0.0.1:15432/storage`). Tests isolate keys with a random prefix and clean up after themselves.

## License

Apache-2.0
