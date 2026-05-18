import Fastify from 'fastify'
import { Registry } from '@platformatic/coordinator'
import { coordinatorPlugin } from '../coordinator-plugin.ts'

const env = (key: string, fallback?: string): string => {
  const v = process.env[key] ?? fallback
  if (v === undefined) throw new Error(`missing env var: ${key}`)
  return v
}

const port = Number(process.env.PORT ?? 8080)
const host = process.env.HOST ?? '0.0.0.0'
const redisUrl = env('REDIS_URL')
const keyPrefix = process.env.KEY_PREFIX ?? 'storage-db'
const strategy = (process.env.STRATEGY ?? 'least-loaded') as 'round-robin' | 'least-loaded' | 'random'

const registry = new Registry({ redis: redisUrl, keyPrefix, strategy })

const app = Fastify({ logger: { level: process.env.LOG_LEVEL ?? 'info' } })
await app.register(coordinatorPlugin, { registry })

const shutdown = async (): Promise<void> => {
  try { await registry.close() } catch { /* ignore */ }
  await app.close()
}

for (const sig of ['SIGINT', 'SIGTERM'] as const) {
  process.once(sig, () => {
    shutdown().then(() => process.exit(0), () => process.exit(1))
  })
}

await app.listen({ port, host })
app.log.info({ port, host, strategy }, 'storage-db coordinator listening')
