import { test, before, after } from 'node:test'
import assert from 'node:assert/strict'
import { spawn, type ChildProcess } from 'node:child_process'
import { setTimeout as wait } from 'node:timers/promises'
import { randomBytes } from 'node:crypto'
import { resolve, dirname } from 'node:path'
import { fileURLToPath } from 'node:url'
import { Redis } from 'iovalkey'
import pg from 'pg'

const REDIS_URL = process.env.REDIS_URL ?? 'redis://127.0.0.1:6390'
const PG_URL = process.env.PG_URL ?? 'postgresql://storage:storage@127.0.0.1:15432/storage'
const KEY_PREFIX = `e2e-${randomBytes(4).toString('hex')}`

const COORDINATOR_PORT = 18080
const POD_PORTS = [13001, 13002, 13003]
const POD_IDS = ['e2e-pod-1', 'e2e-pod-2', 'e2e-pod-3']
const COORDINATOR_URL = `http://127.0.0.1:${COORDINATOR_PORT}`

const exampleDir = resolve(dirname(fileURLToPath(import.meta.url)), '../../examples/storage-db')

const children: ChildProcess[] = []

const baseEnv = {
  ...process.env,
  REDIS_URL,
  KEY_PREFIX,
  LOG_LEVEL: 'warn',
  HEARTBEAT_MS: '500',
  MEMBER_TTL: '3'
}

function spawnNode (script: string, env: Record<string, string>): ChildProcess {
  const child = spawn(process.execPath, [script], {
    cwd: exampleDir,
    env: { ...baseEnv, ...env },
    stdio: ['ignore', 'inherit', 'inherit']
  })
  children.push(child)
  return child
}

async function waitForLiveMembers (n: number, timeoutMs = 10_000): Promise<void> {
  const deadline = Date.now() + timeoutMs
  while (Date.now() < deadline) {
    try {
      const res = await fetch(`${COORDINATOR_URL}/pods`)
      if (res.ok) {
        const body = await res.json() as { count: number }
        if (body.count >= n) return
      }
    } catch { /* not up yet */ }
    await wait(100)
  }
  throw new Error(`timed out waiting for ${n} live members`)
}

async function waitForPort (port: number, path: string, timeoutMs = 10_000): Promise<void> {
  const deadline = Date.now() + timeoutMs
  while (Date.now() < deadline) {
    try {
      const res = await fetch(`http://127.0.0.1:${port}${path}`)
      if (res.ok) return
    } catch { /* not up yet */ }
    await wait(100)
  }
  throw new Error(`timed out waiting for port ${port}${path}`)
}

before(async () => {
  spawnNode('src/bin/coordinator.ts', {
    PORT: String(COORDINATOR_PORT),
    HOST: '127.0.0.1',
    STRATEGY: 'least-loaded',
    CACHE_TTL_MS: '500'
  })

  for (let i = 0; i < POD_PORTS.length; i++) {
    spawnNode('src/bin/pod.ts', {
      PORT: String(POD_PORTS[i]),
      HOST: '127.0.0.1',
      MEMBER_ID: POD_IDS[i],
      MEMBER_ADDRESS: `http://127.0.0.1:${POD_PORTS[i]}`,
      PG_URL
    })
  }

  for (const p of POD_PORTS) await waitForPort(p, '/health')
  await waitForPort(COORDINATOR_PORT, '/pods')
  await waitForLiveMembers(POD_PORTS.length)
})

after(async () => {
  for (const c of children) {
    if (!c.killed) c.kill('SIGTERM')
  }
  await wait(500)
  for (const c of children) {
    if (!c.killed) c.kill('SIGKILL')
  }

  const redis = new Redis(REDIS_URL)
  try {
    const keys = await redis.keys(`${KEY_PREFIX}:*`)
    if (keys.length > 0) await redis.del(...keys)
  } finally {
    await redis.quit()
  }

  const client = new pg.Client({ connectionString: PG_URL })
  await client.connect()
  try {
    const result = await client.query<{ schema_name: string }>(
      "SELECT schema_name FROM information_schema.schemata WHERE schema_name LIKE 'tenant_%'"
    )
    for (const row of result.rows) {
      await client.query(`DROP SCHEMA IF EXISTS "${row.schema_name}" CASCADE`)
    }
  } finally {
    await client.end()
  }
})

test('GET /pods returns the three live pods', async () => {
  const res = await fetch(`${COORDINATOR_URL}/pods`)
  assert.equal(res.status, 200)
  const body = await res.json() as { count: number, members: Array<{ memberId: string }> }
  assert.equal(body.count, 3)
  const ids = body.members.map(m => m.memberId).sort()
  assert.deepEqual(ids, [...POD_IDS].sort())
})

// Runs first among the POST tests so every pod still reports load=0 in Valkey.
// Once any tenant is created and its heartbeat fires (~500ms), the strategy
// would correctly pack new tenants on the remaining cold pod(s) instead of
// distributing - that is the documented behaviour of least-loaded.
test('least-loaded: on cold start, six POSTs distribute across all three pods', async () => {
  const tenantNames = ['cold-1', 'cold-2', 'cold-3', 'cold-4', 'cold-5', 'cold-6']
  const owners: string[] = []
  for (const t of tenantNames) {
    const res = await fetch(`${COORDINATOR_URL}/tenants/${t}`, { method: 'POST' })
    const body = await res.json() as { memberId: string }
    owners.push(body.memberId)
  }
  const unique = new Set(owners)
  assert.equal(unique.size, 3, `expected all 3 pods to be picked, got ${[...unique].join(',')}`)
})

test('POST /tenants/:id picks a pod, binds it, returns the memberId', async () => {
  const res = await fetch(`${COORDINATOR_URL}/tenants/alpha`, { method: 'POST' })
  assert.equal(res.status, 201)
  const body = await res.json() as { tenantId: string, memberId: string }
  assert.equal(body.tenantId, 'alpha')
  assert.ok(POD_IDS.includes(body.memberId), `unexpected memberId ${body.memberId}`)
})

test('PUT then GET round-trips through the same pod', async () => {
  const create = await fetch(`${COORDINATOR_URL}/tenants/bravo`, { method: 'POST' })
  const { memberId: ownerId } = await create.json() as { memberId: string }

  const put = await fetch(`${COORDINATOR_URL}/tenants/bravo/keys/hello`, {
    method: 'PUT',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ value: 'world' })
  })
  assert.equal(put.status, 204)

  const get = await fetch(`${COORDINATOR_URL}/tenants/bravo/keys/hello`)
  assert.equal(get.status, 200)
  const body = await get.json() as { value: string, memberId: string }
  assert.equal(body.value, 'world')
  assert.equal(body.memberId, ownerId)
})

test('postgres state: migrations ran and writes land in the tenant schema', async () => {
  const create = await fetch(`${COORDINATOR_URL}/tenants/pgcheck`, { method: 'POST' })
  assert.equal(create.status, 201)
  const { memberId: owner } = await create.json() as { memberId: string }

  await fetch(`${COORDINATOR_URL}/tenants/pgcheck/keys/alpha`, {
    method: 'PUT',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ value: 'one' })
  })
  await fetch(`${COORDINATOR_URL}/tenants/pgcheck/keys/beta`, {
    method: 'PUT',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ value: 'two' })
  })

  const client = new pg.Client({ connectionString: PG_URL })
  await client.connect()
  try {
    const schemaRows = await client.query<{ schema_name: string }>(
      "SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'tenant_pgcheck'"
    )
    assert.equal(schemaRows.rowCount, 1, 'tenant schema must exist in Postgres')

    const versionRows = await client.query<{ version: string }>(
      'SELECT version FROM tenant_pgcheck.schemaversion ORDER BY version DESC LIMIT 1'
    )
    assert.equal(versionRows.rows[0]?.version, '1', 'postgrator must have applied migration 001')

    const tableRows = await client.query<{ key: string, value: string, updated_at: Date }>(
      'SELECT key, value, updated_at FROM tenant_pgcheck.kv ORDER BY key'
    )
    assert.equal(tableRows.rowCount, 2)
    assert.deepEqual(
      tableRows.rows.map(r => ({ key: r.key, value: r.value })),
      [{ key: 'alpha', value: 'one' }, { key: 'beta', value: 'two' }]
    )
    assert.ok(tableRows.rows[0].updated_at instanceof Date, 'updated_at must be a real timestamp')

    const otherSchema = await client.query(
      "SELECT 1 FROM information_schema.tables WHERE table_schema = 'tenant_pgcheck' AND table_name = 'kv'"
    )
    assert.equal(otherSchema.rowCount, 1)

    assert.ok(POD_IDS.includes(owner))
  } finally {
    await client.end()
  }
})

test('transactions: commit makes the write visible outside the txn', async () => {
  const create = await fetch(`${COORDINATOR_URL}/tenants/txn-commit`, { method: 'POST' })
  const { memberId: owner } = await create.json() as { memberId: string }

  const begin = await fetch(`${COORDINATOR_URL}/tenants/txn-commit/transactions`, { method: 'POST' })
  assert.equal(begin.status, 201)
  const { lockId, memberId: txnOwner } = await begin.json() as { lockId: string, memberId: string }
  assert.equal(txnOwner, owner, 'transaction must start on the tenant-owning pod')

  const outsideBefore = await fetch(`${COORDINATOR_URL}/tenants/txn-commit/keys/k`)
  assert.equal(outsideBefore.status, 404, 'key must not exist before the write')

  const put = await fetch(`${COORDINATOR_URL}/transactions/${lockId}/keys/k`, {
    method: 'PUT',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ value: 'committed-value' })
  })
  assert.equal(put.status, 204)

  const insideRead = await fetch(`${COORDINATOR_URL}/transactions/${lockId}/keys/k`)
  assert.equal(insideRead.status, 200)
  const inside = await insideRead.json() as { value: string, memberId: string, lockId: string }
  assert.equal(inside.value, 'committed-value')
  assert.equal(inside.memberId, owner, 'read must hit the same pod via lock routing')

  const outsideMidTxn = await fetch(`${COORDINATOR_URL}/tenants/txn-commit/keys/k`)
  assert.equal(outsideMidTxn.status, 404, 'uncommitted write must not be visible from outside')

  const commit = await fetch(`${COORDINATOR_URL}/transactions/${lockId}/commit`, { method: 'POST' })
  assert.equal(commit.status, 204)

  const outsideAfter = await fetch(`${COORDINATOR_URL}/tenants/txn-commit/keys/k`)
  assert.equal(outsideAfter.status, 200)
  const after = await outsideAfter.json() as { value: string }
  assert.equal(after.value, 'committed-value')
})

test('transactions: rollback discards the write', async () => {
  await fetch(`${COORDINATOR_URL}/tenants/txn-rollback`, { method: 'POST' })

  await fetch(`${COORDINATOR_URL}/tenants/txn-rollback/keys/k`, {
    method: 'PUT',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ value: 'original' })
  })

  const begin = await fetch(`${COORDINATOR_URL}/tenants/txn-rollback/transactions`, { method: 'POST' })
  const { lockId } = await begin.json() as { lockId: string }

  await fetch(`${COORDINATOR_URL}/transactions/${lockId}/keys/k`, {
    method: 'PUT',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ value: 'overwritten' })
  })

  const insideRead = await fetch(`${COORDINATOR_URL}/transactions/${lockId}/keys/k`)
  const inside = await insideRead.json() as { value: string }
  assert.equal(inside.value, 'overwritten', 'txn sees its own write')

  const rollback = await fetch(`${COORDINATOR_URL}/transactions/${lockId}/rollback`, { method: 'POST' })
  assert.equal(rollback.status, 204)

  const after = await fetch(`${COORDINATOR_URL}/tenants/txn-rollback/keys/k`)
  const body = await after.json() as { value: string }
  assert.equal(body.value, 'original', 'rollback restores the prior value')
})

test('transactions: unknown lockId returns 404', async () => {
  const res = await fetch(`${COORDINATOR_URL}/transactions/tx-does-not-exist/commit`, { method: 'POST' })
  assert.equal(res.status, 404)
  const body = await res.json() as { error: string }
  assert.equal(body.error, 'transaction not found')
})

test('failover (graceful): SIGTERM the owning pod, next request reassigns and preserves data', async () => {
  const create = await fetch(`${COORDINATOR_URL}/tenants/failover-graceful`, { method: 'POST' })
  const { memberId: originalOwner } = await create.json() as { memberId: string }

  const put = await fetch(`${COORDINATOR_URL}/tenants/failover-graceful/keys/k`, {
    method: 'PUT',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ value: 'before-failover' })
  })
  assert.equal(put.status, 204)

  const warmRead = await fetch(`${COORDINATOR_URL}/tenants/failover-graceful/keys/k`)
  const warm = await warmRead.json() as { value: string, memberId: string }
  assert.equal(warm.memberId, originalOwner, 'warmup read must hit the original pod and cache the resolve')

  const ownerIdx = POD_IDS.indexOf(originalOwner)
  assert.ok(ownerIdx >= 0)
  const ownerChild = children[ownerIdx + 1]

  const exited = new Promise<void>((resolve) => ownerChild.once('exit', () => resolve()))
  ownerChild.kill('SIGTERM')
  await Promise.race([exited, wait(5_000)])

  const redis = new Redis(REDIS_URL)
  try {
    const hashExists = await redis.exists(`${KEY_PREFIX}:member:${originalOwner}`)
    assert.equal(hashExists, 0, 'graceful deregister must delete the member hash')
  } finally {
    await redis.quit()
  }

  await wait(700)

  const read = await fetch(`${COORDINATOR_URL}/tenants/failover-graceful/keys/k`)
  assert.equal(read.status, 200)
  const body = await read.json() as { value: string, memberId: string }
  assert.equal(body.value, 'before-failover', 'data must survive the failover')
  assert.notEqual(body.memberId, originalOwner, 'must be served by a different pod')

  const writeAfter = await fetch(`${COORDINATOR_URL}/tenants/failover-graceful/keys/k2`, {
    method: 'PUT',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ value: 'after-failover' })
  })
  assert.equal(writeAfter.status, 204)

  const client = new pg.Client({ connectionString: PG_URL })
  await client.connect()
  try {
    const rows = await client.query<{ key: string, value: string }>(
      'SELECT key, value FROM tenant_failover_graceful.kv ORDER BY key'
    )
    assert.deepEqual(
      rows.rows,
      [{ key: 'k', value: 'before-failover' }, { key: 'k2', value: 'after-failover' }]
    )
  } finally {
    await client.end()
  }
})

test('orphan reassignment: killing the owning pod reroutes the tenant', async () => {
  const create = await fetch(`${COORDINATOR_URL}/tenants/orphan-test`, { method: 'POST' })
  const { memberId: originalOwner } = await create.json() as { memberId: string }

  const ownerIdx = POD_IDS.indexOf(originalOwner)
  assert.ok(ownerIdx >= 0)
  const ownerChild = children[ownerIdx + 1]
  ownerChild.kill('SIGKILL')

  await wait(4_000)

  const res = await fetch(`${COORDINATOR_URL}/tenants/orphan-test/keys/k`, {
    method: 'PUT',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ value: 'after-failover' })
  })
  assert.equal(res.status, 204)

  const get = await fetch(`${COORDINATOR_URL}/tenants/orphan-test/keys/k`)
  const body = await get.json() as { value: string, memberId: string }
  assert.equal(body.value, 'after-failover')
  assert.notEqual(body.memberId, originalOwner)
})
