import fp from 'fastify-plugin'
import type { FastifyInstance, FastifyRequest, FastifyReply } from 'fastify'
import { PoolManager, isValidTenantId } from './pool-manager.ts'

interface TenantParams { tenantId: string }
interface KeyParams { tenantId: string, key: string }
interface ValueBody { value: string }

export interface PodOptions {
  pools: PoolManager
  memberId: string
}

const tenantSchema = {
  params: {
    type: 'object',
    properties: { tenantId: { type: 'string', pattern: '^[a-zA-Z0-9_-]{1,64}$' } },
    required: ['tenantId']
  }
} as const

const keySchema = {
  params: {
    type: 'object',
    properties: {
      tenantId: { type: 'string', pattern: '^[a-zA-Z0-9_-]{1,64}$' },
      key: { type: 'string', minLength: 1, maxLength: 256 }
    },
    required: ['tenantId', 'key']
  }
} as const

async function podRoutes (app: FastifyInstance, opts: PodOptions): Promise<void> {
  const { pools, memberId } = opts

  app.get('/health', async () => ({ ok: true, memberId, load: pools.load(), tenants: pools.tenantIds() }))

  app.post<{ Params: TenantParams }>('/tenants/:tenantId', { schema: tenantSchema }, async (req, reply) => {
    const { tenantId } = req.params
    if (!isValidTenantId(tenantId)) return reply.code(400).send({ error: 'invalid tenantId' })
    await pools.ensure(tenantId)
    return reply.code(201).send({ tenantId, memberId })
  })

  app.put<{ Params: KeyParams, Body: ValueBody }>('/tenants/:tenantId/keys/:key', {
    schema: {
      ...keySchema,
      body: {
        type: 'object',
        properties: { value: { type: 'string' } },
        required: ['value']
      }
    }
  }, async (req, reply) => {
    const { tenantId, key } = req.params
    const pool = await pools.ensure(tenantId)
    const schema = pools.schema(tenantId)
    await pool.query(
      `INSERT INTO "${schema}".kv (key, value)
       VALUES ($1, $2)
       ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value`,
      [key, req.body.value]
    )
    return reply.code(204).send()
  })

  app.get<{ Params: KeyParams }>('/tenants/:tenantId/keys/:key', { schema: keySchema }, async (req, reply) => {
    const { tenantId, key } = req.params
    const pool = await pools.ensure(tenantId)
    const schema = pools.schema(tenantId)
    const result = await pool.query<{ value: string }>(
      `SELECT value FROM "${schema}".kv WHERE key = $1`,
      [key]
    )
    if (result.rowCount === 0) return reply.code(404).send({ error: 'key not found' })
    return { key, value: result.rows[0].value, memberId }
  })

  app.delete<{ Params: KeyParams }>('/tenants/:tenantId/keys/:key', { schema: keySchema }, async (req, reply) => {
    const { tenantId, key } = req.params
    const pool = await pools.ensure(tenantId)
    const schema = pools.schema(tenantId)
    await pool.query(`DELETE FROM "${schema}".kv WHERE key = $1`, [key])
    return reply.code(204).send()
  })

  app.get<{ Params: TenantParams }>('/tenants/:tenantId/keys', { schema: tenantSchema }, async (req) => {
    const { tenantId } = req.params
    const pool = await pools.ensure(tenantId)
    const schema = pools.schema(tenantId)
    const result = await pool.query<{ key: string }>(`SELECT key FROM "${schema}".kv ORDER BY key`)
    return { tenantId, memberId, keys: result.rows.map((r: { key: string }) => r.key) }
  })

  app.delete<{ Params: TenantParams }>('/tenants/:tenantId', { schema: tenantSchema }, async (req: FastifyRequest, reply: FastifyReply) => {
    const { tenantId } = req.params as TenantParams
    await pools.drop(tenantId)
    return reply.code(204).send()
  })
}

export const podPlugin = fp(podRoutes, { name: 'storage-db-pod' })
