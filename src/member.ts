import { Redis } from 'iovalkey'

export interface MemberOptions {
  redis: string
  memberId: string
  address: string
  keyPrefix?: string
  ttl?: number
}

export class Member {
  #redis: Redis
  #memberId: string
  #address: string
  #keyPrefix: string
  #ttl: number

  constructor (opts: MemberOptions) {
    this.#redis = new Redis(opts.redis)
    this.#memberId = opts.memberId
    this.#address = opts.address
    this.#keyPrefix = opts.keyPrefix ?? 'coordinator'
    this.#ttl = opts.ttl ?? 30
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

  #memberKey (): string {
    return `${this.#keyPrefix}:member:${this.#memberId}`
  }

  #instanceKey (instanceId: string): string {
    return `${this.#keyPrefix}:instance:${instanceId}`
  }

  #memberLoadKey (): string {
    return `${this.#keyPrefix}:member:${this.#memberId}:instances`
  }

  async register (): Promise<void> {
    await this.#redis.sadd(this.#membersKey(), this.#memberId)
    await this.#redis.set(this.#memberKey(), this.#address, 'EX', this.#ttl)
    await this.#redis.set(this.#memberLoadKey(), '0', 'EX', this.#ttl)
  }

  async deregister (): Promise<void> {
    await this.#redis.srem(this.#membersKey(), this.#memberId)
    await this.#redis.del(this.#memberKey(), this.#memberLoadKey())
  }

  async heartbeat (): Promise<void> {
    await this.#redis.expire(this.#memberKey(), this.#ttl)
    await this.#redis.expire(this.#memberLoadKey(), this.#ttl)
  }

  async registerInstance (instanceId: string): Promise<void> {
    await this.#redis.set(this.#instanceKey(instanceId), this.#memberId)
    await this.#redis.incr(this.#memberLoadKey())
  }

  async deregisterInstance (instanceId: string): Promise<void> {
    await this.#redis.del(this.#instanceKey(instanceId))
    await this.#redis.decr(this.#memberLoadKey())
  }

  async lookupInstance (instanceId: string): Promise<string | null> {
    const memberId = await this.#redis.get(this.#instanceKey(instanceId))
    if (!memberId) return null
    return this.#redis.get(`${this.#keyPrefix}:member:${memberId}`)
  }

  async close (): Promise<void> {
    await this.#redis.quit()
  }
}
