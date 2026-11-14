import type { Redis, RedisOptions } from 'iovalkey'
import { connect, type RedisConnection } from './connection.ts'
import type { MemberInfo } from './strategies.ts'

export interface MemberOptions {
  /** A Valkey URL, or an existing client which is not closed by close(). */
  redis: RedisConnection
  /** Options for the client created from a URL. */
  redisOptions?: RedisOptions
  memberId: string
  address: string
  keyPrefix?: string
  ttl?: number
  getLoad?: () => number
  /** Maintains the set of the destinations of this member, see the `memberIndex` option of `Registry`. */
  memberIndex?: boolean
}

export class Member {
  #redis: Redis
  #ownsRedis: boolean
  #memberId: string
  #address: string
  #keyPrefix: string
  #ttl: number
  #getLoad: () => number
  #memberIndex: boolean
  #draining: boolean

  constructor (opts: MemberOptions) {
    const { redis, owned } = connect(opts.redis, opts.redisOptions)
    this.#redis = redis
    this.#ownsRedis = owned
    this.#memberId = opts.memberId
    this.#address = opts.address
    this.#keyPrefix = opts.keyPrefix ?? 'coordinator'
    this.#ttl = opts.ttl ?? 30
    this.#getLoad = opts.getLoad ?? (() => 0)
    this.#memberIndex = opts.memberIndex ?? false
    this.#draining = false
  }

  get memberId (): string {
    return this.#memberId
  }

  get address (): string {
    return this.#address
  }

  #membersKey (): string {
    return `${this.#keyPrefix}:members`
  }

  #memberKey (memberId: string = this.#memberId): string {
    return `${this.#keyPrefix}:member:${memberId}`
  }

  #destinationKey (destId: string): string {
    return `${this.#keyPrefix}:destination:${destId}`
  }

  #ownedKey (): string {
    return `${this.#keyPrefix}:owned:${this.#memberId}`
  }

  #lockKey (lockId: string): string {
    return `${this.#keyPrefix}:lock:${lockId}`
  }

  async register (): Promise<void> {
    await this.#writeRecord()
  }

  // Rewrites the whole record, so that a member whose record expired (for instance while Valkey was unreachable for
  // longer than the TTL) becomes live again instead of keeping a record without an address.
  async heartbeat (): Promise<void> {
    await this.#writeRecord()
  }

  async #writeRecord (): Promise<void> {
    const pipeline = this.#redis.pipeline()
    pipeline.sadd(this.#membersKey(), this.#memberId)
    // A record written again after it expired keeps the member draining
    pipeline.hset(this.#memberKey(), {
      address: this.#address,
      load: String(this.#getLoad()),
      ...(this.#draining ? { draining: '1' } : {})
    })
    pipeline.expire(this.#memberKey(), this.#ttl)
    await pipeline.exec()
  }

  async deregister (): Promise<void> {
    const pipeline = this.#redis.pipeline()
    pipeline.srem(this.#membersKey(), this.#memberId)
    pipeline.del(this.#memberKey())
    await pipeline.exec()
  }

  // A draining member keeps the destinations it holds, but is bound to no new one, nor reassigned any, until it
  // resumes: the registry checks the flag atomically when it binds. The heartbeats keep the flag, and write it again
  // with a record which expired; it goes when the member resumes or deregisters.
  async setDraining (draining: boolean): Promise<void> {
    this.#draining = draining

    if (draining) {
      await this.#redis.hset(this.#memberKey(), 'draining', '1')
    } else {
      await this.#redis.hdel(this.#memberKey(), 'draining')
    }
  }

  get draining (): boolean {
    return this.#draining
  }

  async addToDestination (destinationId: string): Promise<void> {
    if (this.#memberIndex) {
      await this.#redis.multi().sadd(this.#destinationKey(destinationId), this.#memberId).sadd(this.#ownedKey(), destinationId).exec()
    } else {
      await this.#redis.sadd(this.#destinationKey(destinationId), this.#memberId)
    }
  }

  async removeFromDestination (destinationId: string): Promise<void> {
    if (this.#memberIndex) {
      await this.#redis.multi().srem(this.#destinationKey(destinationId), this.#memberId).srem(this.#ownedKey(), destinationId).exec()
    } else {
      await this.#redis.srem(this.#destinationKey(destinationId), this.#memberId)
    }
  }

  // The destinations of this member, from the member index
  async listOwned (): Promise<string[]> {
    return this.#redis.smembers(this.#ownedKey())
  }

  async registerLock (
    lockId: string,
    destinationId: string,
    metadata: Record<string, string> = {}
  ): Promise<void> {
    await this.#redis.hset(this.#lockKey(lockId), {
      podId: this.#memberId,
      destinationId,
      ...metadata
    })
  }

  async unregisterLock (lockId: string): Promise<void> {
    await this.#redis.del(this.#lockKey(lockId))
  }

  async listPeerLoad (): Promise<MemberInfo[]> {
    const memberIds = await this.#redis.smembers(this.#membersKey())
    if (memberIds.length === 0) return []

    const pipeline = this.#redis.pipeline()
    for (const id of memberIds) {
      pipeline.hmget(this.#memberKey(id), 'address', 'load', 'draining')
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
      live.push({ memberId: memberIds[i], address, load, draining: fields[2] === '1' })
    }
    return live
  }

  async close (): Promise<void> {
    if (this.#ownsRedis) await this.#redis.quit()
  }
}
