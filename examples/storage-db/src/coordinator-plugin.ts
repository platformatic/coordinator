import fp from 'fastify-plugin'
import type { FastifyInstance, FastifyRequest } from 'fastify'
import { type Registry, coordinatorPlugin } from '@platformatic/coordinator'

interface TenantParams { tenantId: string }
interface LockParams { lockId: string }

export interface CoordinatorOptions {
  registry: Registry
}

const tenantSchema = {
  params: {
    type: 'object',
    properties: { tenantId: { type: 'string', pattern: '^[a-zA-Z0-9_-]{1,64}$' } },
    required: ['tenantId']
  }
} as const

const tenantKeySchema = {
  params: {
    type: 'object',
    properties: {
      tenantId: { type: 'string', pattern: '^[a-zA-Z0-9_-]{1,64}$' },
      key: { type: 'string', minLength: 1, maxLength: 256 }
    },
    required: ['tenantId', 'key']
  }
} as const

const lockSchema = {
  params: {
    type: 'object',
    properties: { lockId: { type: 'string', minLength: 1, maxLength: 128 } },
    required: ['lockId']
  }
} as const

const lockKeySchema = {
  params: {
    type: 'object',
    properties: {
      lockId: { type: 'string', minLength: 1, maxLength: 128 },
      key: { type: 'string', minLength: 1, maxLength: 256 }
    },
    required: ['lockId', 'key']
  }
} as const

async function storageDbRoutes (app: FastifyInstance, opts: CoordinatorOptions): Promise<void> {
  await app.register(coordinatorPlugin, { registry: opts.registry })

  const tenantFrom = (req: FastifyRequest): string => (req.params as TenantParams).tenantId
  const lockFrom = (req: FastifyRequest): string => (req.params as LockParams).lockId

  app.get('/pods', async () => {
    const members = await app.coordinator.registry.listLiveMembers()
    return { count: members.length, members }
  })

  app.post('/tenants/:tenantId', { schema: tenantSchema }, app.coordinator.pickAndRegister({
    registerIdFrom: (body: any) => body.tenantId,
    expectedStatus: 201,
    unavailableMessage: 'no pods available'
  }))

  const tenantProxy = app.coordinator.lookupAndProxy({
    destinationFrom: tenantFrom,
    reassignOrphans: true,
    notFoundMessage: 'tenant not found'
  })

  app.get('/tenants/:tenantId/keys', { schema: tenantSchema }, tenantProxy)
  app.get('/tenants/:tenantId/keys/:key', { schema: tenantKeySchema }, tenantProxy)
  app.put('/tenants/:tenantId/keys/:key', { schema: tenantKeySchema }, tenantProxy)
  app.delete('/tenants/:tenantId/keys/:key', { schema: tenantKeySchema }, tenantProxy)

  app.delete('/tenants/:tenantId', { schema: tenantSchema }, app.coordinator.lookupAndDeregister({
    destinationFrom: tenantFrom,
    notFoundMessage: 'tenant not found'
  }))

  app.post('/tenants/:tenantId/transactions',
    { schema: tenantSchema },
    tenantProxy)

  const lockProxy = app.coordinator.lookupLockAndProxy({
    lockFrom,
    notFoundMessage: 'transaction not found'
  })

  app.put('/transactions/:lockId/keys/:key', { schema: lockKeySchema }, lockProxy)
  app.get('/transactions/:lockId/keys/:key', { schema: lockKeySchema }, lockProxy)
  app.post('/transactions/:lockId/commit', { schema: lockSchema }, lockProxy)
  app.post('/transactions/:lockId/rollback', { schema: lockSchema }, lockProxy)
}

export const storageDbCoordinatorPlugin = fp(storageDbRoutes, { name: 'storage-db-coordinator' })
