import { Redis, type RedisOptions } from 'iovalkey'

export type RedisConnection = string | Redis

export interface Connection {
  redis: Redis
  owned: boolean
}

// A URL creates a client owned by the caller, closed on close(). An existing client is shared and left open, so that
// it can also run the application's own commands and settings (timeouts, retries, TLS) stay under its control.
export function connect (redis: RedisConnection, redisOptions?: RedisOptions): Connection {
  if (typeof redis === 'string') {
    return { redis: redisOptions ? new Redis(redis, redisOptions) : new Redis(redis), owned: true }
  }

  return { redis, owned: false }
}
