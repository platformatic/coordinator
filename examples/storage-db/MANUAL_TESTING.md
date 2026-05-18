# Manual Testing

How to spin up the storage-db stack by hand and exercise it from a shell. The automated e2e suite at `coordinator/test/e2e/storage-db.test.ts` covers the same paths in CI; this document is for when you want to poke at it interactively.

The end state in both paths below is the same: a coordinator listening on `localhost:8080`, three pods registered in Valkey, Postgres ready for tenant data.

## Path 1 - everything in containers

Fastest. One command:

```sh
cd examples/storage-db
docker compose up --build -d --wait
```

Brings up:

| Service | Host port | Role |
|---|---|---|
| `valkey` | `6379` | member registry + destination sets + lock records |
| `postgres` | `5432` | shared database, one schema per tenant |
| `coordinator` | `8080` | the HTTP front door |
| `pod1`, `pod2`, `pod3` | not published | storage pods, reachable from inside the compose network |

Run the bundled exerciser:

```sh
./scripts/smoke.sh
```

Tear down (also wipes Valkey + Postgres state):

```sh
docker compose down -v
```

## Path 2 - node processes against containerized Valkey + Postgres

Useful when you want to attach a debugger, edit code, and see changes without a Docker rebuild.

Start only the data stores:

```sh
cd examples/storage-db
docker compose up -d --wait valkey postgres
```

From the workspace root (the `coordinator/` directory), install and build the library:

```sh
cd ../..
pnpm install
pnpm run build
```

Then run each process in its own terminal:

```sh
cd examples/storage-db

# Terminal 1: coordinator on :8080
REDIS_URL=redis://127.0.0.1:6379 \
  STRATEGY=least-loaded \
  PORT=8080 \
  node src/bin/coordinator.ts

# Terminal 2: pod1 on :3001
REDIS_URL=redis://127.0.0.1:6379 \
  PG_URL=postgresql://storage:storage@127.0.0.1:5432/storage \
  MEMBER_ID=pod1 MEMBER_ADDRESS=http://127.0.0.1:3001 \
  PORT=3001 \
  node src/bin/pod.ts

# Terminal 3: pod2 on :3002 (same as pod1 but PORT=3002, MEMBER_ID=pod2, MEMBER_ADDRESS=http://127.0.0.1:3002)
# Terminal 4: pod3 on :3003
```

The smoke script and the curl examples below work against either path because both expose the coordinator on `localhost:8080`.

## Exercise the API

```sh
# List the live pods
curl -s localhost:8080/pods | jq

# Create a tenant. The coordinator picks a pod via the configured strategy
# (default: least-loaded) and returns the chosen memberId.
curl -s -X POST localhost:8080/tenants/foo | jq

# Write a key
curl -s -X PUT localhost:8080/tenants/foo/keys/hello \
     -H 'content-type: application/json' \
     -d '{"value":"world"}'

# Read it back. The response is tagged with the serving pod's memberId.
# All reads/writes for the same tenant route to the same pod.
curl -s localhost:8080/tenants/foo/keys/hello | jq

# List all keys for a tenant
curl -s localhost:8080/tenants/foo/keys | jq

# Drop the tenant (DROP SCHEMA CASCADE + remove the destination set)
curl -s -X DELETE localhost:8080/tenants/foo
```

### Transactions

```sh
# Begin a transaction. Returns { lockId, tenantId, memberId }.
# The pod opens a pinned pg.PoolClient, runs BEGIN, registers the lock in Valkey.
curl -s -X POST localhost:8080/tenants/foo/transactions | jq

# Capture the lockId for the next steps
LOCK=$(curl -s -X POST localhost:8080/tenants/foo/transactions | jq -r .lockId)

# Write inside the transaction. The coordinator routes this via lookupLockAndProxy
# to the same pod that holds the pinned connection.
curl -s -X PUT localhost:8080/transactions/$LOCK/keys/k \
     -H 'content-type: application/json' \
     -d '{"value":"inside-txn"}'

# Read inside the transaction sees the uncommitted write
curl -s localhost:8080/transactions/$LOCK/keys/k | jq

# Read outside the transaction does NOT see the uncommitted write
curl -s localhost:8080/tenants/foo/keys/k | jq
# -> 404 until commit

# Commit. The pod runs COMMIT, releases the pinned connection, unregisters the lock.
curl -s -X POST localhost:8080/transactions/$LOCK/commit

# Now the read from outside sees the value
curl -s localhost:8080/tenants/foo/keys/k | jq

# Or rollback instead:
# curl -s -X POST localhost:8080/transactions/$LOCK/rollback
```

## Inspect Valkey while the stack runs

```sh
# Container-local CLI
docker compose exec valkey valkey-cli

# Or from the host (Valkey port is published)
redis-cli -h 127.0.0.1 -p 6379
```

Useful reads:

```sh
docker compose exec valkey valkey-cli SMEMBERS storage-db:members
docker compose exec valkey valkey-cli HGETALL storage-db:member:pod1
docker compose exec valkey valkey-cli TTL storage-db:member:pod1
docker compose exec valkey valkey-cli SMEMBERS storage-db:destination:foo
docker compose exec valkey valkey-cli KEYS 'storage-db:lock:*'
docker compose exec valkey valkey-cli HGETALL storage-db:lock:<lockId>

# Live tail of every Valkey command (heartbeats, resolves, lock writes)
docker compose exec valkey valkey-cli MONITOR
```

## Inspect Postgres directly

Bypass the pod and the coordinator to verify what actually landed in the database:

```sh
docker compose exec postgres psql -U storage -d storage
```

Inside psql:

```sql
-- List tenant schemas
SELECT schema_name FROM information_schema.schemata WHERE schema_name LIKE 'tenant_%';

-- Postgrator's migration log for one tenant
SELECT * FROM tenant_foo.schemaversion;

-- The actual data table
SELECT key, value, updated_at FROM tenant_foo.kv;
```

## Tail logs

```sh
docker compose logs -f coordinator
docker compose logs -f pod1
docker compose logs -f valkey postgres   # combined data-store logs
```

## Walkthrough: failover when a pod dies

End-to-end demonstration that a tenant survives its pod going away. Assumes a clean stack (`docker compose down -v && docker compose up --build -d --wait`).

### 1. Create a tenant and write data

```sh
ASSIGNED=$(curl -s -X POST localhost:8080/tenants/foo | jq -r .memberId)
echo "Tenant foo is on $ASSIGNED"

curl -s -X PUT localhost:8080/tenants/foo/keys/k \
     -H 'content-type: application/json' \
     -d '{"value":"before-failover"}'

curl -s localhost:8080/tenants/foo/keys/k | jq
# { "key": "k", "value": "before-failover", "memberId": "<ASSIGNED>" }
```

Confirm Valkey and Postgres both reflect the new state:

```sh
docker compose exec valkey valkey-cli SMEMBERS storage-db:destination:foo
# 1) "<ASSIGNED>"

docker compose exec postgres psql -U storage -d storage \
  -c "SELECT key, value FROM tenant_foo.kv"
#  key |      value
# -----+-----------------
#  k   | before-failover
```

### 2. Stop the assigned pod

```sh
docker compose stop "$ASSIGNED"
```

`docker compose stop` sends `SIGTERM`. The pod's signal handler runs `Member.deregister()`, which:

- removes the pod from `storage-db:members` (the live-pod set)
- deletes `storage-db:member:<ASSIGNED>` (the hash holding `address` and `load`)

```sh
docker compose exec valkey valkey-cli SMEMBERS storage-db:members
# (the other two pods, no $ASSIGNED)

docker compose exec valkey valkey-cli EXISTS "storage-db:member:$ASSIGNED"
# (integer) 0

# But the destination set still says the tenant is on the dead pod:
docker compose exec valkey valkey-cli SMEMBERS storage-db:destination:foo
# 1) "<ASSIGNED>"
```

The destination set isn't cleaned up at shutdown because the registry has no reverse index from pod -> destinations it serves. The cleanup happens lazily on the next request, in the orphan-reassign code path.

### 3. Wait for the coordinator's resolve cache to expire

The coordinator caches `resolveDestination` results for 5 seconds by default. While that entry is warm, requests for `foo` will still try the dead address and fail with a connection error. Wait past the TTL:

```sh
sleep 6
```

(Or `docker compose restart coordinator` to wipe the cache immediately. The e2e suite sets `CACHE_TTL_MS=500` for the same reason.)

### 4. Trigger the failover with a real request

```sh
curl -s localhost:8080/tenants/foo/keys/k | jq
# {
#   "key": "k",
#   "value": "before-failover",
#   "memberId": "<NEW POD>"
# }
```

Three things happened inside that one HTTP call:

1. Coordinator cache miss -> fresh `resolveDestination("foo")`.
2. Destination set was `[<ASSIGNED>]`, member hash gone, so `livePods=[]`, `deadPods=[<ASSIGNED>]`. With `reassignOrphans: true`, the coordinator picked a live pod via the strategy, did `SREM <ASSIGNED>` and `SADD <new pod>` on `destination:foo`, returned the new pod.
3. The new pod's PUT/GET handler called `pools.ensure("foo")`. `CREATE SCHEMA IF NOT EXISTS tenant_foo` was a no-op, postgrator saw schemaversion at version 1 and ran no migrations. `SELECT FROM tenant_foo.kv` returned the existing row.

The destination set in Valkey now reflects the new assignment:

```sh
docker compose exec valkey valkey-cli SMEMBERS storage-db:destination:foo
# 1) "<new pod>"
```

### 5. Verify the new pod owns the tenant

```sh
docker compose exec coordinator wget -qO- "http://<new pod>:3000/health" | jq
# { "ok": true, "memberId": "<new pod>", "load": 1, "tenants": ["foo"] }
```

### 6. Write more data; it lands in the same schema

```sh
curl -s -X PUT localhost:8080/tenants/foo/keys/k2 \
     -H 'content-type: application/json' \
     -d '{"value":"after-failover"}'

docker compose exec postgres psql -U storage -d storage \
  -c "SELECT key, value FROM tenant_foo.kv ORDER BY key"
#  key |      value
# -----+-----------------
#  k   | before-failover
#  k2  | after-failover
```

The data persists across the pod loss because storage-db is schema-per-tenant in a **shared** Postgres database. The pod owns the connections; the data lives in Postgres.

### 7. Restore the original pod (optional)

```sh
docker compose start "$ASSIGNED"
```

It re-registers itself within ~10 seconds and joins `storage-db:members` as available capacity for new tenants. The existing `foo` tenant stays on the new pod (there's no rebalancing on rejoin).

### Variant: hard kill instead of graceful stop

```sh
docker compose kill "$ASSIGNED"
```

`SIGKILL` skips the deregister handler. The member hash stays in Valkey until its TTL expires (default `30s`, set via `MEMBER_TTL`). During that window the next request still routes to the dead pod's address and times out. After TTL expires, orphan-reassign kicks in. The e2e suite uses `MEMBER_TTL=3` to make this fast; for an interactive demo you can either wait, or rebuild the image with a shorter `MEMBER_TTL` in `docker-compose.yml`.

## Things worth observing

- `GET /pods` shows `load` per pod (sum of open Postgres connections across its tenant pools). Watch it climb after writes, then settle to idle after `idleTimeoutMillis` (60s).
- After several `POST /tenants/...` calls, the `load` field starts to diverge between pods. Subsequent tenants land on whichever pod has the lowest `load`.
- `docker compose restart pod2` -> the pod re-registers itself in Valkey within seconds; tenants pinned to pod2 keep routing to it as soon as it's back.
- `docker compose stop pod3` -> after ~30s (the default `MEMBER_TTL`), pod3's hash record expires in Valkey; tenants that were on pod3 reassign to a live pod on their next request (orphan reassignment).
- `docker compose exec valkey valkey-cli FLUSHALL` -> everything reassigns: pods will re-register on the next heartbeat, but every destination set is gone, so the next request for a tenant returns 404 (no claim-on-miss in this example).
