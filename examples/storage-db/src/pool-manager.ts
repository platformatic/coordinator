import { randomUUID } from 'node:crypto'
import { fileURLToPath } from 'node:url'
import { dirname, resolve } from 'node:path'
import pg from 'pg'
import Postgrator from 'postgrator'

const { Pool } = pg

const MIGRATION_DIR = resolve(dirname(fileURLToPath(import.meta.url)), '../migrations')

export interface PoolManagerOptions {
  connectionString: string
  max?: number
  migrationDir?: string
}

export interface TransactionHandle {
  lockId: string
  client: pg.PoolClient
  tenantId: string
  schema: string
}

export class PoolManager {
  #connectionString: string
  #max: number
  #migrationDir: string
  #pools = new Map<string, pg.Pool>()
  #transactions = new Map<string, TransactionHandle>()

  constructor (opts: PoolManagerOptions) {
    this.#connectionString = opts.connectionString
    this.#max = opts.max ?? 5
    this.#migrationDir = opts.migrationDir ?? MIGRATION_DIR
  }

  async ensure (tenantId: string): Promise<pg.Pool> {
    let pool = this.#pools.get(tenantId)
    if (pool) return pool

    pool = new Pool({
      connectionString: this.#connectionString,
      max: this.#max,
      idleTimeoutMillis: 60_000
    })

    const schema = schemaName(tenantId)
    const client = await pool.connect()
    try {
      await client.query(`CREATE SCHEMA IF NOT EXISTS ${quoteIdent(schema)}`)
      await client.query(`SET search_path TO ${quoteIdent(schema)}`)

      const postgrator = new Postgrator({
        migrationPattern: `${this.#migrationDir}/*`,
        driver: 'pg',
        schemaTable: `${schema}.schemaversion`,
        currentSchema: schema,
        execQuery: (query: string) => client.query(query)
      })
      await postgrator.migrate()
    } finally {
      client.release()
    }

    this.#pools.set(tenantId, pool)
    return pool
  }

  pool (tenantId: string): pg.Pool | undefined {
    return this.#pools.get(tenantId)
  }

  async drop (tenantId: string): Promise<void> {
    const pool = this.#pools.get(tenantId)
    if (!pool) return
    this.#pools.delete(tenantId)
    try {
      const client = await pool.connect()
      try {
        await client.query(`DROP SCHEMA IF EXISTS ${quoteIdent(schemaName(tenantId))} CASCADE`)
      } finally {
        client.release()
      }
    } finally {
      await pool.end()
    }
  }

  schema (tenantId: string): string {
    return schemaName(tenantId)
  }

  async beginTransaction (tenantId: string): Promise<TransactionHandle> {
    const pool = await this.ensure(tenantId)
    const client = await pool.connect()
    try {
      const schema = schemaName(tenantId)
      await client.query(`SET search_path TO ${quoteIdent(schema)}`)
      await client.query('BEGIN')
      const lockId = `tx-${randomUUID()}`
      const handle: TransactionHandle = { lockId, client, tenantId, schema }
      this.#transactions.set(lockId, handle)
      return handle
    } catch (err) {
      client.release()
      throw err
    }
  }

  transaction (lockId: string): TransactionHandle | undefined {
    return this.#transactions.get(lockId)
  }

  async commitTransaction (lockId: string): Promise<TransactionHandle | null> {
    const handle = this.#transactions.get(lockId)
    if (!handle) return null
    this.#transactions.delete(lockId)
    try {
      await handle.client.query('COMMIT')
    } finally {
      handle.client.release()
    }
    return handle
  }

  async rollbackTransaction (lockId: string): Promise<TransactionHandle | null> {
    const handle = this.#transactions.get(lockId)
    if (!handle) return null
    this.#transactions.delete(lockId)
    try {
      await handle.client.query('ROLLBACK')
    } finally {
      handle.client.release()
    }
    return handle
  }

  load (): number {
    let total = 0
    for (const pool of this.#pools.values()) {
      total += pool.totalCount
    }
    return total
  }

  tenantIds (): string[] {
    return [...this.#pools.keys()]
  }

  async close (): Promise<void> {
    for (const handle of this.#transactions.values()) {
      try { await handle.client.query('ROLLBACK') } catch { /* ignore */ }
      handle.client.release()
    }
    this.#transactions.clear()

    const pools = [...this.#pools.values()]
    this.#pools.clear()
    await Promise.all(pools.map(p => p.end()))
  }
}

const tenantPattern = /^[a-zA-Z0-9_-]{1,64}$/

export function isValidTenantId (id: string): boolean {
  return tenantPattern.test(id)
}

function schemaName (tenantId: string): string {
  if (!isValidTenantId(tenantId)) {
    throw new Error(`invalid tenantId: ${tenantId}`)
  }
  return `tenant_${tenantId.replace(/-/g, '_')}`
}

function quoteIdent (ident: string): string {
  return `"${ident.replace(/"/g, '""')}"`
}
