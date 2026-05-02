import { Redis } from 'iovalkey'
import { createStrategy, type AllocationStrategy, type MemberWithLoad } from './strategies.ts'

export interface MemberInfo {
  memberId: string
  address: string
}

export interface RegistryOptions {
  redis: string
  keyPrefix?: string
  strategy?: 'round-robin' | 'least-loaded' | 'random' | AllocationStrategy
  requestTimeout?: number
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

  constructor (opts: RegistryOptions) {
    this.#redis = new Redis(opts.redis)
    this.#keyPrefix = opts.keyPrefix ?? 'coordinator'

    if (typeof opts.strategy === 'object' && opts.strategy !== null) {
      this.#strategy = opts.strategy
    } else {
      this.#strategy = createStrategy(opts.strategy ?? 'round-robin')
    }

    this.requestTimeout = opts.requestTimeout
  }

  #membersKey (): string {
    return `${this.#keyPrefix}:members`
  }

  #memberKey (memberId: string): string {
    return `${this.#keyPrefix}:member:${memberId}`
  }

  #instanceKey (instanceId: string): string {
    return `${this.#keyPrefix}:instance:${instanceId}`
  }

  #memberLoadKey (memberId: string): string {
    return `${this.#keyPrefix}:member:${memberId}:instances`
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
      instanceCount: parseInt(counts[i] ?? '0', 10) || 0
    }))
  }

  async pickMember (): Promise<MemberInfo | null> {
    const members = await this.listMembersWithLoad()
    return this.#strategy.pick(members)
  }

  async lookupInstance (instanceId: string): Promise<string | null> {
    const memberId = await this.#redis.get(this.#instanceKey(instanceId))
    if (!memberId) return null
    return this.#redis.get(this.#memberKey(memberId))
  }

  async lookupInstanceMemberId (instanceId: string): Promise<string | null> {
    return this.#redis.get(this.#instanceKey(instanceId))
  }

  async resolveInstance (
    instanceId: string,
    opts: { reassignOrphans?: boolean } = {}
  ): Promise<ResolveResult | null> {
    const memberId = await this.#redis.get(this.#instanceKey(instanceId))
    if (!memberId) return null

    const address = await this.#redis.get(this.#memberKey(memberId))
    if (address) return { address, reassigned: false }

    if (!opts.reassignOrphans) {
      return { address: null, reassigned: false }
    }

    const newPod = await this.pickMember()
    if (!newPod) return { address: null, reassigned: false }

    await this.registerInstance(instanceId, newPod.memberId)
    return { address: newPod.address, reassigned: true }
  }

  async registerInstance (instanceId: string, memberId: string): Promise<void> {
    await this.#redis.set(this.#instanceKey(instanceId), memberId)
  }

  async deregisterInstance (instanceId: string): Promise<void> {
    await this.#redis.del(this.#instanceKey(instanceId))
  }

  async close (): Promise<void> {
    await this.#redis.quit()
  }
}
