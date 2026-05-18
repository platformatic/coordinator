# @platformatic/storage-db

A reference *headless service* for the `@platformatic/coordinator` pattern: a multi-tenant Postgres KV store split across N pods, with each pod opening one Postgres connection pool **per tenant it serves**. Each pod self-registers in Valkey; a coordinator process routes tenant traffic to the right pod.

This repo exists to demonstrate that the coordinator pattern is platform-neutral (no Kubernetes required) and to give a working end-to-end smoke test you can run with one docker-compose command.

## What's inside

| Process | Role | Port |
|---|---|---|
| `valkey` | Member registry + destination sets | internal |
| `postgres` | Shared Postgres, one schema per tenant | internal |
| `coordinator` | Resolves tenant -> pod via `@platformatic/coordinator`, proxies HTTP | `8080` (host) |
| `pod1` / `pod2` / `pod3` | Storage pods: open a `pg.Pool` per tenant lazily on first hit, report `load = total open connections` | internal |

## Run

```sh
docker compose up --build
```

Then in a second terminal:

```sh
./scripts/smoke.sh
```

You should see five tenants get spread across the three pods (default strategy is `least-loaded`), and reads come back tagged with the `memberId` of the pod that served them.

## API (via the coordinator at `:8080`)

| Method | Path | What it does |
|---|---|---|
| `GET`  | `/pods` | List live pods + their current `load` |
| `POST` | `/tenants/:tenantId` | Pick a pod, create the tenant on it, bind tenant -> pod in Valkey |
| `PUT`  | `/tenants/:tenantId/keys/:key` | Upsert `{ value }` for the tenant's owning pod |
| `GET`  | `/tenants/:tenantId/keys/:key` | Read a single key |
| `GET`  | `/tenants/:tenantId/keys` | List all keys for the tenant |
| `DELETE` | `/tenants/:tenantId/keys/:key` | Delete one key |
| `DELETE` | `/tenants/:tenantId` | Drop the tenant entirely + remove the binding |

`tenantId` must match `^[a-zA-Z0-9_-]{1,64}$`.

## How the pieces map to the coordinator pattern

| Abstract concept (see `coordinator/coordinator-pattern.md`) | storage-db |
|---|---|
| Resource pod | One `pod*` container running `src/bin/pod.ts` |
| Destination | A `tenantId` |
| Local share | The per-tenant `pg.Pool` on that pod |
| `load` | Sum of `pool.totalCount` across the pod's tenant pools |
| Coordinator | `coordinator` container running `src/bin/coordinator.ts` |
| Member registry | Valkey, key prefix `storage-db:` |

## Layout

```
src/
  bin/
    pod.ts             # pod entry point (Member.register + heartbeat + Fastify)
    coordinator.ts     # coordinator entry point (Registry + helpers + Fastify)
  pod-plugin.ts        # /tenants/:id and /keys routes for the pod
  coordinator-plugin.ts# routes that use pickAndRegister / lookupAndProxy / lookupAndDeregister
  pool-manager.ts      # per-tenant pg.Pool, lazy creation, load = total open connections
scripts/smoke.sh       # end-to-end exerciser
docker-compose.yml     # valkey + postgres + coordinator + 3 pods
Dockerfile             # one image, two entry points (pod, coordinator)
```

## Environment variables

### Pod (`src/bin/pod.ts`)
- `PORT` (default `3000`)
- `HOST` (default `0.0.0.0`)
- `REDIS_URL` (required)
- `MEMBER_ID` (required) - opaque, stable across the pod's lifetime
- `MEMBER_ADDRESS` (required) - the URL the coordinator will dial, e.g. `http://pod1:3000`
- `PG_URL` (required)
- `KEY_PREFIX` (default `storage-db`)
- `HEARTBEAT_MS` (default `10000`)

### Coordinator (`src/bin/coordinator.ts`)
- `PORT` (default `8080`)
- `HOST` (default `0.0.0.0`)
- `REDIS_URL` (required)
- `KEY_PREFIX` (default `storage-db`)
- `STRATEGY` (default `least-loaded`) - one of `round-robin`, `least-loaded`, `random`

## Notes

- This is a demo, not production code. There is no auth, no TLS, no metrics.
- Tenant deletion drops the Postgres schema with `CASCADE`. Don't point this at a database you care about.
- The pod uses `--experimental-strip-types` to run TypeScript directly; no build step is needed for the demo.
