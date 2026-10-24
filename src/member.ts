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
}

export class Member {
  #redis: Redis
  #ownsRedis: boolean
  #memberId: string
  #address: string
  #keyPrefix: string
  #ttl: number
  #getLoad: () => number

  constructor (opts: MemberOptions) {
    const { redis, owned } = connect(opts.redis, opts.redisOptions)
    this.#redis = redis
    this.#ownsRedis = owned
    this.#memberId = opts.memberId
    this.#address = opts.address
    this.#keyPrefix = opts.keyPrefix ?? 'coordinator'
    this.#ttl = opts.ttl ?? 30
    this.#getLoad = opts.getLoad ?? (() => 0)
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
    pipeline.hset(this.#memberKey(), {
      address: this.#address,
      load: String(this.#getLoad())
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

  async addToDestination (destinationId: string): Promise<void> {
    await this.#redis.sadd(this.#destinationKey(destinationId), this.#memberId)
  }

  async removeFromDestination (destinationId: string): Promise<void> {
    await this.#redis.srem(this.#destinationKey(destinationId), this.#memberId)
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

  async close (): Promise<void> {
    if (this.#ownsRedis) await this.#redis.quit()
  }
}
