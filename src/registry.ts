import type { Redis, RedisOptions } from 'iovalkey'
import { connect, type RedisConnection } from './connection.ts'
import { createStrategy, type AllocationStrategy, type MemberInfo } from './strategies.ts'
import { TTLCache, type CacheOptions } from './cache.ts'

export interface RegistryOptions {
  /** A Valkey URL, or an existing client which is not closed by close(). */
  redis: RedisConnection
  /** Options for the client created from a URL. */
  redisOptions?: RedisOptions
  keyPrefix?: string
  strategy?: 'round-robin' | 'least-loaded' | 'random' | AllocationStrategy
  requestTimeout?: number
  cache?: CacheOptions | false
}

export interface ResolveResult {
  address: string
  memberId: string
  reassigned: boolean
  /** When reassigned, the dead members which were removed from the destination. */
  previous?: string[]
}

export interface ResolveLockResult {
  address: string
  memberId: string
}

// Resolves a destination atomically. Live members are returned as flat [memberId, address, load] triples, and dead
// members are pruned when at least one is live. Without a candidate (ARGV[2]) it only reports how many members the
// set holds; with one, it binds the candidate when the set is empty (ARGV[3] = '1', claim) or when every member is dead
// (ARGV[4] = '1', reassign). Concurrent resolutions therefore agree: a caller losing the race gets the members bound by
// the winner instead of adding its own candidate.
const resolveScript = `
  local members = redis.call('SMEMBERS', KEYS[1])
  local live = {}
  local dead = {}

  for _, member in ipairs(members) do
    local fields = redis.call('HMGET', ARGV[1] .. member, 'address', 'load')

    if fields[1] then
      table.insert(live, member)
      table.insert(live, fields[1])
      table.insert(live, fields[2] or '0')
    else
      table.insert(dead, member)
    end
  end

  if #live > 0 then
    if #dead > 0 then
      redis.call('SREM', KEYS[1], unpack(dead))
    end

    return { 'live', live }
  end

  local allowed = (#members == 0 and ARGV[3] == '1') or (#members > 0 and ARGV[4] == '1')

  if ARGV[2] == '' or not allowed then
    return { 'empty', #members }
  end

  local address = redis.call('HGET', ARGV[1] .. ARGV[2], 'address')

  if not address then
    return { 'stale' }
  end

  redis.call('DEL', KEYS[1])
  redis.call('SADD', KEYS[1], ARGV[2])

  return { 'bound', { ARGV[2], address }, dead }
`

// A candidate can expire between the pick and the script, in which case another one is picked
const maxBindAttempts = 3

type ResolveReply =
  | ['live', string[]]
  | ['empty', number]
  | ['stale']
  | ['bound', [string, string], string[]]

interface ResolveCommand {
  coordinatorResolve (key: string, memberPrefix: string, candidate: string, claim: string, reassign: string): Promise<ResolveReply>
}

export class Registry {
  #redis: Redis & ResolveCommand
  #ownsRedis: boolean
  #keyPrefix: string
  #strategy: AllocationStrategy
  #cache: TTLCache<string, ResolveResult> | null
  readonly requestTimeout: number | undefined

  constructor (opts: RegistryOptions) {
    const { redis, owned } = connect(opts.redis, opts.redisOptions)
    redis.defineCommand('coordinatorResolve', { numberOfKeys: 1, lua: resolveScript })
    this.#redis = redis as Redis & ResolveCommand
    this.#ownsRedis = owned
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
    const key = this.#destinationKey(destinationId)
    const memberPrefix = this.#memberKey('')
    const claim = opts.claimOnMiss ? '1' : '0'
    const reassign = opts.reassignOrphans ? '1' : '0'

    let reply = await this.#redis.coordinatorResolve(key, memberPrefix, '', claim, reassign)

    for (let attempt = 0; attempt < maxBindAttempts; attempt++) {
      if (reply[0] === 'live') {
        return this.#pickLive(destinationId, reply[1])
      }

      if (reply[0] === 'bound') {
        const [, [memberId, address], dead] = reply
        return dead.length > 0
          ? { address, memberId, reassigned: true, previous: dead }
          : { address, memberId, reassigned: false }
      }

      if (reply[0] === 'empty') {
        const wanted = reply[1] === 0 ? opts.claimOnMiss : opts.reassignOrphans
        if (!wanted) return null
      }

      const picked = await this.pickMember({ destinationId })
      if (!picked) return null

      reply = await this.#redis.coordinatorResolve(key, memberPrefix, picked.memberId, claim, reassign)
    }

    return reply[0] === 'live' ? this.#pickLive(destinationId, reply[1]) : null
  }

  #pickLive (destinationId: string, triples: string[]): ResolveResult | null {
    const live: MemberInfo[] = []
    for (let i = 0; i < triples.length; i += 3) {
      live.push({ memberId: triples[i], address: triples[i + 1], load: parseInt(triples[i + 2], 10) || 0 })
    }
    // A destination bound to a single live pod does not consult the strategy, so that lookups do not advance stateful
    // strategies like round-robin, which would skew the picks for new destinations
    const picked = live.length === 1 ? live[0] : this.#strategy.pick(live, { destinationId })
    if (!picked) return null
    return { address: picked.address, memberId: picked.memberId, reassigned: false }
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
    if (this.#ownsRedis) await this.#redis.quit()
  }
}
