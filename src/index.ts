export { Registry } from './registry.ts'
export type { RegistryOptions, MemberInfo, ResolveResult } from './registry.ts'

export { Member } from './member.ts'
export type { MemberOptions } from './member.ts'

export {
  RoundRobinStrategy,
  LeastLoadedStrategy,
  RandomStrategy,
  createStrategy
} from './strategies.ts'
export type { AllocationStrategy, MemberWithLoad } from './strategies.ts'

export { proxyRequest } from './proxy-request.ts'
export type { ProxyRequestOptions } from './proxy-request.ts'

export { lookupAndProxy } from './helpers/lookup-and-proxy.ts'
export type { LookupAndProxyOptions, LookupAndProxyResult } from './helpers/lookup-and-proxy.ts'

export { pickAndRegister } from './helpers/pick-and-register.ts'
export type { PickAndRegisterOptions, PickAndRegisterResult } from './helpers/pick-and-register.ts'

export { lookupAndDeregister } from './helpers/lookup-and-deregister.ts'
export type { LookupAndDeregisterOptions, LookupAndDeregisterResult } from './helpers/lookup-and-deregister.ts'
