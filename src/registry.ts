import { Redis } from 'iovalkey'
import { createStrategy, type AllocationStrategy, type MemberWithLoad } from './strategies.ts'
import type { CoordinatorMetrics } from './metrics.ts'

export interface MemberInfo {
  memberId: string
  address: string
}

export interface RegistryOptions {
  redis: string
  keyPrefix?: string
  strategy?: 'round-robin' | 'least-loaded' | 'random' | AllocationStrategy
  requestTimeout?: number
  metrics?: CoordinatorMetrics
}

export interface ResolveResult {
  address: string | null
  reassigned: boolean
}

export class Registry {
  #redis: Redis
  #keyPrefix: string
  #strategy: AllocationStrategy

  readonly requestTimeout: number | undefined
  readonly metrics: CoordinatorMetrics | undefined

  constructor (opts: RegistryOptions) {
    this.#redis = new Redis(opts.redis)
    this.#keyPrefix = opts.keyPrefix ?? 'coordinator'

    if (typeof opts.strategy === 'object' && opts.strategy !== null) {
      this.#strategy = opts.strategy
    } else {
      this.#strategy = createStrategy(opts.strategy ?? 'round-robin')
    }

    this.requestTimeout = opts.requestTimeout
    this.metrics = opts.metrics
  }

  #membersKey (): string {
    return `${this.#keyPrefix}:members`
  }

  #memberKey (memberId: string): string {
    return `${this.#keyPrefix}:member:${memberId}`
  }

  #resourceKey (resourceId: string): string {
    return `${this.#keyPrefix}:resource:${resourceId}`
  }

  #memberLoadKey (memberId: string): string {
    return `${this.#keyPrefix}:member:${memberId}:resources`
  }

  async listMembers (): Promise<MemberInfo[]> {
    const memberIds = await this.#redis.smembers(this.#membersKey())
    const members: MemberInfo[] = []
    for (const memberId of memberIds) {
      const address = await this.#redis.get(this.#memberKey(memberId))
      if (address) {
        members.push({ memberId, address })
      }
    }
    return members
  }

  async listMembersWithLoad (): Promise<MemberWithLoad[]> {
    const members = await this.listMembers()
    if (members.length === 0) return []

    const countKeys = members.map(m => this.#memberLoadKey(m.memberId))
    const counts = await this.#redis.mget(...countKeys)

    return members.map((member, i) => ({
      ...member,
      resourceCount: parseInt(counts[i] ?? '0', 10) || 0
    }))
  }

  async pickMember (): Promise<MemberInfo | null> {
    const members = await this.listMembersWithLoad()
    return this.#strategy.pick(members)
  }

  async lookupResource (resourceId: string): Promise<string | null> {
    const memberId = await this.#redis.get(this.#resourceKey(resourceId))
    if (!memberId) return null
    return this.#redis.get(this.#memberKey(memberId))
  }

  async lookupResourceMemberId (resourceId: string): Promise<string | null> {
    return this.#redis.get(this.#resourceKey(resourceId))
  }

  async resolveResource (
    resourceId: string,
    opts: { reassignOrphans?: boolean } = {}
  ): Promise<ResolveResult | null> {
    const memberId = await this.#redis.get(this.#resourceKey(resourceId))
    if (!memberId) return null

    const address = await this.#redis.get(this.#memberKey(memberId))
    if (address) return { address, reassigned: false }

    if (!opts.reassignOrphans) {
      return { address: null, reassigned: false }
    }

    const newPod = await this.pickMember()
    if (!newPod) return { address: null, reassigned: false }

    await this.registerResource(resourceId, newPod.memberId)
    return { address: newPod.address, reassigned: true }
  }

  async registerResource (resourceId: string, memberId: string): Promise<void> {
    await this.#redis.set(this.#resourceKey(resourceId), memberId)
  }

  async deregisterResource (resourceId: string): Promise<void> {
    await this.#redis.del(this.#resourceKey(resourceId))
  }

  async close (): Promise<void> {
    await this.#redis.quit()
  }
}
