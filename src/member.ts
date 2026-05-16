import { Redis } from 'iovalkey'
import type { MemberInfo } from './strategies.ts'

export interface MemberOptions {
  redis: string
  memberId: string
  address: string
  keyPrefix?: string
  ttl?: number
  getTotalConnections?: () => number
}

export class Member {
  #redis: Redis
  #memberId: string
  #address: string
  #keyPrefix: string
  #ttl: number
  #getTotalConnections: () => number

  constructor (opts: MemberOptions) {
    this.#redis = new Redis(opts.redis)
    this.#memberId = opts.memberId
    this.#address = opts.address
    this.#keyPrefix = opts.keyPrefix ?? 'coordinator'
    this.#ttl = opts.ttl ?? 30
    this.#getTotalConnections = opts.getTotalConnections ?? (() => 0)
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
    const pipeline = this.#redis.pipeline()
    pipeline.sadd(this.#membersKey(), this.#memberId)
    pipeline.hset(this.#memberKey(), {
      address: this.#address,
      total_connections: String(this.#getTotalConnections())
    })
    pipeline.expire(this.#memberKey(), this.#ttl)
    await pipeline.exec()
  }

  async heartbeat (): Promise<void> {
    const pipeline = this.#redis.pipeline()
    pipeline.hset(this.#memberKey(), 'total_connections', String(this.#getTotalConnections()))
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

  async close (): Promise<void> {
    await this.#redis.quit()
  }
}
