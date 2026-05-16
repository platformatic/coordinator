import { Redis } from 'iovalkey'
import { createStrategy, type AllocationStrategy, type MemberInfo } from './strategies.ts'
import { TTLCache, type CacheOptions } from './cache.ts'

export interface RegistryOptions {
  redis: string
  keyPrefix?: string
  strategy?: 'round-robin' | 'least-loaded' | 'random' | AllocationStrategy
  requestTimeout?: number
  cache?: CacheOptions | false
}

export interface ResolveResult {
  address: string
  memberId: string
  reassigned: boolean
}

export interface ResolveLockResult {
  address: string
  memberId: string
}

interface MemberRecord {
  memberId: string
  address: string | null
  totalConnections: number
}

export class Registry {
  #redis: Redis
  #keyPrefix: string
  #strategy: AllocationStrategy
  #cache: TTLCache<string, ResolveResult> | null
  readonly requestTimeout: number | undefined

  constructor (opts: RegistryOptions) {
    this.#redis = new Redis(opts.redis)
    this.#keyPrefix = opts.keyPrefix ?? 'coordinator'

    if (typeof opts.strategy === 'object' && opts.strategy !== null) {
      this.#strategy = opts.strategy
    } else {
      this.#strategy = createStrategy(opts.strategy ?? 'round-robin')
    }

    this.#cache = opts.cache === false
      ? null
      : new TTLCache<string, ResolveResult>(opts.cache)
    this.requestTimeout = opts.requestTimeout
  }

  #membersKey (): string {
    return `${this.#keyPrefix}:members`
  }

  #memberKey (memberId: string): string {
    return `${this.#keyPrefix}:member:${memberId}`
  }

  #destinationKey (destId: string): string {
    return `${this.#keyPrefix}:destination:${destId}`
  }

  #lockKey (lockId: string): string {
    return `${this.#keyPrefix}:lock:${lockId}`
  }

  async listLiveMembers (): Promise<MemberInfo[]> {
    const memberIds = await this.#redis.smembers(this.#membersKey())
    if (memberIds.length === 0) return []

    const pipeline = this.#redis.pipeline()
    for (const id of memberIds) {
      pipeline.hmget(this.#memberKey(id), 'address', 'total_connections')
    }
    const results = await pipeline.exec()
    if (!results) return []

    const live: MemberInfo[] = []
    for (let i = 0; i < memberIds.length; i++) {
      const [err, fields] = results[i] as [Error | null, (string | null)[] | null]
      if (err || !fields) continue
      const address = fields[0]
      if (!address) continue
      const totalConnections = parseInt(fields[1] ?? '0', 10) || 0
      live.push({ memberId: memberIds[i], address, totalConnections })
    }
    return live
  }

  async pickMember (ctx: { instanceId?: string } = {}): Promise<MemberInfo | null> {
    const live = await this.listLiveMembers()
    return this.#strategy.pick(live, ctx)
  }

  async resolveInstance (
    instanceId: string,
    opts: { claimOnMiss?: boolean, reassignOrphans?: boolean } = {}
  ): Promise<ResolveResult | null> {
    if (this.#cache) {
      const cached = this.#cache.get(instanceId)
      if (cached) return cached
    }
    const result = await this.#resolveInstanceUncached(instanceId, opts)
    if (this.#cache && result) this.#cache.set(instanceId, result)
    return result
  }

  async #resolveInstanceUncached (
    instanceId: string,
    opts: { claimOnMiss?: boolean, reassignOrphans?: boolean }
  ): Promise<ResolveResult | null> {
    const podIds = await this.#redis.smembers(this.#destinationKey(instanceId))

    if (podIds.length === 0) {
      if (!opts.claimOnMiss) return null
      const live = await this.listLiveMembers()
      if (live.length === 0) return null
      const picked = this.#strategy.pick(live, { instanceId })
      if (!picked) return null
      await this.#redis.sadd(this.#destinationKey(instanceId), picked.memberId)
      return { address: picked.address, memberId: picked.memberId, reassigned: false }
    }

    const records = await this.#getMemberRecords(podIds)
    const livePods: MemberInfo[] = []
    const deadPods: string[] = []
    for (const r of records) {
      if (r.address) {
        livePods.push({ memberId: r.memberId, address: r.address, totalConnections: r.totalConnections })
      } else {
        deadPods.push(r.memberId)
      }
    }

    if (livePods.length > 0) {
      if (deadPods.length > 0) {
        this.#redis.srem(this.#destinationKey(instanceId), ...deadPods).catch(() => {})
      }
      const picked = this.#strategy.pick(livePods, { instanceId })
      if (!picked) return null
      return { address: picked.address, memberId: picked.memberId, reassigned: false }
    }

    if (!opts.reassignOrphans) return null
    const live = await this.listLiveMembers()
    if (live.length === 0) return null
    const picked = this.#strategy.pick(live, { instanceId })
    if (!picked) return null

    const pipeline = this.#redis.pipeline()
    for (const deadId of deadPods) {
      pipeline.srem(this.#destinationKey(instanceId), deadId)
    }
    pipeline.sadd(this.#destinationKey(instanceId), picked.memberId)
    await pipeline.exec()

    return { address: picked.address, memberId: picked.memberId, reassigned: true }
  }

  async #getMemberRecords (memberIds: string[]): Promise<MemberRecord[]> {
    if (memberIds.length === 0) return []
    const pipeline = this.#redis.pipeline()
    for (const id of memberIds) {
      pipeline.hmget(this.#memberKey(id), 'address', 'total_connections')
    }
    const results = await pipeline.exec()
    if (!results) return memberIds.map(memberId => ({ memberId, address: null, totalConnections: 0 }))

    return memberIds.map((memberId, i) => {
      const [err, fields] = results[i] as [Error | null, (string | null)[] | null]
      if (err || !fields) return { memberId, address: null, totalConnections: 0 }
      const address = fields[0] ?? null
      const totalConnections = parseInt(fields[1] ?? '0', 10) || 0
      return { memberId, address, totalConnections }
    })
  }

  async addPodToDestination (instanceId: string, memberId: string): Promise<void> {
    await this.#redis.sadd(this.#destinationKey(instanceId), memberId)
    if (this.#cache) this.#cache.delete(instanceId)
  }

  async hasBinding (instanceId: string): Promise<boolean> {
    const count = await this.#redis.scard(this.#destinationKey(instanceId))
    return count > 0
  }

  async resolveLock (lockId: string): Promise<ResolveLockResult | null> {
    const podId = await this.#redis.hget(this.#lockKey(lockId), 'podId')
    if (!podId) return null
    const address = await this.#redis.hget(this.#memberKey(podId), 'address')
    if (!address) return null
    return { address, memberId: podId }
  }

  invalidateCache (instanceId: string): void {
    if (this.#cache) this.#cache.delete(instanceId)
  }

  async deregisterInstance (instanceId: string): Promise<void> {
    await this.#redis.del(this.#destinationKey(instanceId))
    if (this.#cache) this.#cache.delete(instanceId)
  }

  async close (): Promise<void> {
    await this.#redis.quit()
  }
}
