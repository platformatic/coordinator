import fp from 'fastify-plugin'
import replyFrom from '@fastify/reply-from'
import type { FastifyInstance, FastifyRequest } from 'fastify'
import {
  Registry,
  lookupAndProxy,
  pickAndRegister,
  lookupAndDeregister
} from '@platformatic/coordinator'

interface TenantParams { tenantId: string }

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

async function coordinatorRoutes (app: FastifyInstance, opts: CoordinatorOptions): Promise<void> {
  const { registry } = opts
  await app.register(replyFrom)

  const tenantFrom = (req: FastifyRequest): string => (req.params as TenantParams).tenantId

  app.get('/pods', async () => {
    const members = await registry.listLiveMembers()
    return { count: members.length, members }
  })

  app.post('/tenants/:tenantId', { schema: tenantSchema }, pickAndRegister(registry, {
    registerIdFrom: (body: any) => body.tenantId,
    expectedStatus: 201,
    unavailableMessage: 'no pods available'
  }))

  const proxyOpts = {
    destinationFrom: tenantFrom,
    reassignOrphans: true,
    notFoundMessage: 'tenant not found'
  }

  app.get('/tenants/:tenantId/keys', { schema: tenantSchema }, lookupAndProxy(registry, proxyOpts))
  app.get('/tenants/:tenantId/keys/:key', { schema: tenantKeySchema }, lookupAndProxy(registry, proxyOpts))
  app.put('/tenants/:tenantId/keys/:key', { schema: tenantKeySchema }, lookupAndProxy(registry, proxyOpts))
  app.delete('/tenants/:tenantId/keys/:key', { schema: tenantKeySchema }, lookupAndProxy(registry, proxyOpts))

  app.delete('/tenants/:tenantId', { schema: tenantSchema }, lookupAndDeregister(registry, {
    destinationFrom: tenantFrom,
    notFoundMessage: 'tenant not found'
  }))
}

export const coordinatorPlugin = fp(coordinatorRoutes, { name: 'storage-db-coordinator' })
