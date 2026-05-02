export interface CoordinatorMetrics {
  requestsTotal: { inc: (labels: { route: string, result: string }) => void }
  podCount: { set: (value: number) => void }
}

export interface CreateMetricsOptions {
  namespace?: string
}

export function createCoordinatorMetrics (
  prometheus?: { client: any, registry: any },
  opts: CreateMetricsOptions = {}
): CoordinatorMetrics | null {
  const p = prometheus ?? (globalThis as any).platformatic?.prometheus
  if (!p) return null

  const namespace = opts.namespace ?? 'coordinator'
  const { client, registry } = p

  const requestsTotal = new client.Counter({
    name: `${namespace}_requests_total`,
    help: 'Coordinator routed requests',
    labelNames: ['route', 'result'],
    registers: [registry]
  })

  const podCount = new client.Gauge({
    name: `${namespace}_pod_count`,
    help: 'Registered pod count',
    registers: [registry]
  })

  return { requestsTotal, podCount }
}
