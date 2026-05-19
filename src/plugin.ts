import type { FastifyPluginAsync, RouteHandlerMethod } from 'fastify'
import fp from 'fastify-plugin'
import replyFrom, { type FastifyReplyFromOptions } from '@fastify/reply-from'
import { Registry, type RegistryOptions } from './registry.ts'
import { lookupAndProxy, type LookupAndProxyOptions } from './helpers/lookup-and-proxy.ts'
import { lookupLockAndProxy, type LookupLockAndProxyOptions } from './helpers/lookup-lock-and-proxy.ts'
import { pickAndRegister, type PickAndRegisterOptions } from './helpers/pick-and-register.ts'
import { lookupAndDeregister, type LookupAndDeregisterOptions } from './helpers/lookup-and-deregister.ts'
import { proxyVia, type ProxyResolver, type ProxyTarget, type ProxyViaOptions } from './helpers/proxy-via.ts'

export interface Coordinator {
  /** Underlying Registry instance; use it for non-Fastify operations (listLiveMembers, etc.). */
  registry: Registry
  lookupAndProxy: (opts: LookupAndProxyOptions) => RouteHandlerMethod
  lookupLockAndProxy: (opts: LookupLockAndProxyOptions) => RouteHandlerMethod
  pickAndRegister: (opts: PickAndRegisterOptions) => RouteHandlerMethod
  lookupAndDeregister: (opts: LookupAndDeregisterOptions) => RouteHandlerMethod
  proxyVia: <T extends ProxyTarget = ProxyTarget> (resolve: ProxyResolver<T>, opts?: ProxyViaOptions) => RouteHandlerMethod
}

export interface CoordinatorPluginOptions extends Partial<RegistryOptions> {
  /**
   * Reuse an existing Registry instance instead of creating one from the
   * remaining options. When provided, the plugin does NOT close it on shutdown
   * (lifecycle stays with the caller).
   *
   * When omitted, `redis` is required.
   */
  registry?: Registry
  /**
   * Forwarded to `@fastify/reply-from` registration.
   */
  replyFrom?: FastifyReplyFromOptions
  /**
   * Name of the decorator that exposes the Coordinator on the Fastify instance.
   * Defaults to `coordinator`.
   */
  decorateAs?: string
  /**
   * Set to `false` if the host app has already registered `@fastify/reply-from`
   * (or a compatible plugin providing `reply.from`). Defaults to `true`.
   */
  registerReplyFrom?: boolean
}

declare module 'fastify' {
  interface FastifyInstance {
    coordinator: Coordinator
  }
}

const plugin: FastifyPluginAsync<CoordinatorPluginOptions> = async (app, opts) => {
  const {
    registry: externalRegistry,
    replyFrom: replyFromOpts,
    decorateAs = 'coordinator',
    registerReplyFrom = true,
    ...registryOpts
  } = opts

  if (registerReplyFrom && !app.hasReplyDecorator('from')) {
    if (replyFromOpts) {
      await app.register(replyFrom, replyFromOpts)
    } else {
      await app.register(replyFrom)
    }
  }

  const ownsRegistry = !externalRegistry
  if (!externalRegistry && !registryOpts.redis) {
    throw new Error('coordinatorPlugin requires either `redis` or `registry`')
  }
  const registry = externalRegistry ?? new Registry(registryOpts as RegistryOptions)

  const coordinator: Coordinator = {
    registry,
    lookupAndProxy: (o) => lookupAndProxy(registry, o),
    lookupLockAndProxy: (o) => lookupLockAndProxy(registry, o),
    pickAndRegister: (o) => pickAndRegister(registry, o),
    lookupAndDeregister: (o) => lookupAndDeregister(registry, o),
    proxyVia: (resolve, o) => proxyVia(resolve, o)
  }

  app.decorate(decorateAs, coordinator)

  if (ownsRegistry) {
    app.addHook('onClose', async () => { await registry.close() })
  }
}

export default fp(plugin, {
  name: '@platformatic/coordinator',
  fastify: '5.x'
})

export { plugin as coordinatorPlugin }
