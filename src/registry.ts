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
  load: number
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
      pipeline.hmget(this.#memberKey(id), 'address', 'load')
    }
    const results = await pipeline.exec()
    if (!results) return []

    const live: MemberInfo[] = []
    for (let i = 0; i < memberIds.length; i++) {
      const [err, fields] = results[i] as [Error | null, (string | null)[] | null]
      if (err || !fields) continue
      const address = fields[0]
      if (!address) continue
      const load = parseInt(fields[1] ?? '0', 10) || 0
      live.push({ memberId: memberIds[i], address, load })
    }
    return live
  }

  async pickMember (ctx: { destinationId?: string } = {}): Promise<MemberInfo | null> {
    const live = await this.listLiveMembers()
    return this.#strategy.pick(live, ctx)
  }

  async resolveDestination (
    destinationId: string,
    opts: { claimOnMiss?: boolean, reassignOrphans?: boolean } = {}
  ): Promise<ResolveResult | null> {
    if (this.#cache) {
      const cached = this.#cache.get(destinationId)
      if (cached) return cached
    }
    const result = await this.#resolveDestinationUncached(destinationId, opts)
    if (this.#cache && result) this.#cache.set(destinationId, result)
    return result
  }

  async #resolveDestinationUncached (
    destinationId: string,
    opts: { claimOnMiss?: boolean, reassignOrphans?: boolean }
  ): Promise<ResolveResult | null> {
    const podIds = await this.#redis.smembers(this.#destinationKey(destinationId))

    if (podIds.length === 0) {
      if (!opts.claimOnMiss) return null
      const live = await this.listLiveMembers()
      if (live.length === 0) return null
      const picked = this.#strategy.pick(live, { destinationId })
      if (!picked) return null
      await this.#redis.sadd(this.#destinationKey(destinationId), picked.memberId)
      return { address: picked.address, memberId: picked.memberId, reassigned: false }
    }

    const records = await this.#getMemberRecords(podIds)
    const livePods: MemberInfo[] = []
    const deadPods: string[] = []
    for (const r of records) {
      if (r.address) {
        livePods.push({ memberId: r.memberId, address: r.address, load: r.load })
      } else {
        deadPods.push(r.memberId)
      }
    }

    if (livePods.length > 0) {
      if (deadPods.length > 0) {
        this.#redis.srem(this.#destinationKey(destinationId), ...deadPods).catch(() => {})
      }
      const picked = this.#strategy.pick(livePods, { destinationId })
      if (!picked) return null
      return { address: picked.address, memberId: picked.memberId, reassigned: false }
    }

    if (!opts.reassignOrphans) return null
    const live = await this.listLiveMembers()
    if (live.length === 0) return null
    const picked = this.#strategy.pick(live, { destinationId })
    if (!picked) return null

    const pipeline = this.#redis.pipeline()
    for (const deadId of deadPods) {
      pipeline.srem(this.#destinationKey(destinationId), deadId)
    }
    pipeline.sadd(this.#destinationKey(destinationId), picked.memberId)
    await pipeline.exec()

    return { address: picked.address, memberId: picked.memberId, reassigned: true }
  }

  async #getMemberRecords (memberIds: string[]): Promise<MemberRecord[]> {
    if (memberIds.length === 0) return []
    const pipeline = this.#redis.pipeline()
    for (const id of memberIds) {
      pipeline.hmget(this.#memberKey(id), 'address', 'load')
    }
    const results = await pipeline.exec()
    if (!results) return memberIds.map(memberId => ({ memberId, address: null, load: 0 }))

    return memberIds.map((memberId, i) => {
      const [err, fields] = results[i] as [Error | null, (string | null)[] | null]
      if (err || !fields) return { memberId, address: null, load: 0 }
      const address = fields[0] ?? null
      const load = parseInt(fields[1] ?? '0', 10) || 0
      return { memberId, address, load }
    })
  }

  async addPodToDestination (destinationId: string, memberId: string): Promise<void> {
    await this.#redis.sadd(this.#destinationKey(destinationId), memberId)
    if (this.#cache) this.#cache.delete(destinationId)
  }

  async hasBinding (destinationId: string): Promise<boolean> {
    const count = await this.#redis.scard(this.#destinationKey(destinationId))
    return count > 0
  }

  async resolveLock (lockId: string): Promise<ResolveLockResult | null> {
    const podId = await this.#redis.hget(this.#lockKey(lockId), 'podId')
    if (!podId) return null
    const address = await this.#redis.hget(this.#memberKey(podId), 'address')
    if (!address) return null
    return { address, memberId: podId }
  }

  invalidateCache (destinationId: string): void {
    if (this.#cache) this.#cache.delete(destinationId)
  }

  async deregisterDestination (destinationId: string): Promise<void> {
    await this.#redis.del(this.#destinationKey(destinationId))
    if (this.#cache) this.#cache.delete(destinationId)
  }

  async close (): Promise<void> {
    await this.#redis.quit()
  }
}
