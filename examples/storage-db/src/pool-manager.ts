import pg from 'pg'

const { Pool } = pg

export interface PoolManagerOptions {
  connectionString: string
  max?: number
}

export class PoolManager {
  #connectionString: string
  #max: number
  #pools = new Map<string, pg.Pool>()

  constructor (opts: PoolManagerOptions) {
    this.#connectionString = opts.connectionString
    this.#max = opts.max ?? 5
  }

  has (tenantId: string): boolean {
    return this.#pools.has(tenantId)
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
      await client.query(
        `CREATE TABLE IF NOT EXISTS ${quoteIdent(schema)}.kv (
           key text PRIMARY KEY,
           value text NOT NULL
         )`
      )
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
