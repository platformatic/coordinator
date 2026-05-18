import { test, before, after } from 'node:test'
import assert from 'node:assert/strict'
import { spawn, type ChildProcess } from 'node:child_process'
import { setTimeout as wait } from 'node:timers/promises'
import { randomBytes } from 'node:crypto'
import { resolve, dirname } from 'node:path'
import { fileURLToPath } from 'node:url'
import { Redis } from 'iovalkey'
import pg from 'pg'

const REDIS_URL = process.env.REDIS_URL ?? 'redis://127.0.0.1:6379'
const PG_URL = process.env.PG_URL ?? 'postgresql://storage:storage@127.0.0.1:5432/storage'
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
    STRATEGY: 'least-loaded'
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

test('least-loaded spreads new tenants across pods', async () => {
  const tenantNames = ['t1', 't2', 't3', 't4', 't5', 't6']
  const owners: string[] = []
  for (const t of tenantNames) {
    const res = await fetch(`${COORDINATOR_URL}/tenants/${t}`, { method: 'POST' })
    const body = await res.json() as { memberId: string }
    owners.push(body.memberId)
  }
  const unique = new Set(owners)
  assert.ok(unique.size >= 2, `expected >=2 distinct pods, got ${[...unique].join(',')}`)
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
