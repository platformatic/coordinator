import Fastify from 'fastify'
import { Member } from '@platformatic/coordinator'
import { PoolManager } from '../pool-manager.ts'
import { podPlugin } from '../pod-plugin.ts'

const env = (key: string, fallback?: string): string => {
  const v = process.env[key] ?? fallback
  if (v === undefined) throw new Error(`missing env var: ${key}`)
  return v
}

const port = Number(process.env.PORT ?? 3000)
const host = process.env.HOST ?? '0.0.0.0'
const redisUrl = env('REDIS_URL')
const memberId = env('MEMBER_ID')
const memberAddress = env('MEMBER_ADDRESS')
const pgUrl = env('PG_URL')
const keyPrefix = process.env.KEY_PREFIX ?? 'storage-db'
const heartbeatMs = Number(process.env.HEARTBEAT_MS ?? 10_000)
const memberTtl = process.env.MEMBER_TTL ? Number(process.env.MEMBER_TTL) : undefined

const pools = new PoolManager({ connectionString: pgUrl })

const member = new Member({
  redis: redisUrl,
  memberId,
  address: memberAddress,
  keyPrefix,
  ttl: memberTtl,
  getLoad: () => pools.load()
})

const app = Fastify({ logger: { level: process.env.LOG_LEVEL ?? 'info' } })
await app.register(podPlugin, { pools, member, memberId })

await member.register()
app.log.info({ memberId, memberAddress }, 'registered in member registry')

const heartbeat = setInterval(() => {
  member.heartbeat().catch(err => app.log.error({ err }, 'heartbeat failed'))
}, heartbeatMs)
heartbeat.unref()

const shutdown = async (): Promise<void> => {
  clearInterval(heartbeat)
  try { await member.deregister() } catch { /* ignore */ }
  try { await member.close() } catch { /* ignore */ }
  try { await pools.close() } catch { /* ignore */ }
  await app.close()
}

for (const sig of ['SIGINT', 'SIGTERM'] as const) {
  process.once(sig, () => {
    shutdown().then(() => process.exit(0), () => process.exit(1))
  })
}

await app.listen({ port, host })
app.log.info({ port, host }, 'storage-db pod listening')
