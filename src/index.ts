export { Registry } from './registry.ts'
export type { RegistryOptions, ResolveResult, ResolveLockResult } from './registry.ts'

export { Member } from './member.ts'
export type { MemberOptions } from './member.ts'

export {
  RoundRobinStrategy,
  LeastLoadedStrategy,
  RandomStrategy,
  createStrategy
} from './strategies.ts'
export type { AllocationStrategy, MemberInfo, PickContext } from './strategies.ts'

export { TTLCache } from './cache.ts'
export type { CacheOptions } from './cache.ts'

export { proxyRequest } from './proxy-request.ts'
export type { ProxyRequestOptions } from './proxy-request.ts'

export { lookupAndProxy } from './helpers/lookup-and-proxy.ts'
export type { LookupAndProxyOptions, LookupAndProxyResult } from './helpers/lookup-and-proxy.ts'

export { pickAndRegister } from './helpers/pick-and-register.ts'
export type { PickAndRegisterOptions, PickAndRegisterResult } from './helpers/pick-and-register.ts'

export { lookupAndDeregister } from './helpers/lookup-and-deregister.ts'
export type { LookupAndDeregisterOptions, LookupAndDeregisterResult } from './helpers/lookup-and-deregister.ts'

export { lookupLockAndProxy } from './helpers/lookup-lock-and-proxy.ts'
export type { LookupLockAndProxyOptions, LookupLockAndProxyResult } from './helpers/lookup-lock-and-proxy.ts'

export { proxyVia } from './helpers/proxy-via.ts'
export type { ProxyViaOptions, ProxyResolver, ProxyTarget } from './helpers/proxy-via.ts'

export { default as coordinatorPlugin } from './plugin.ts'
export type { CoordinatorPluginOptions } from './plugin.ts'
