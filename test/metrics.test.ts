import { strictEqual, ok } from 'node:assert'
import test from 'node:test'
import { createCoordinatorMetrics } from '../src/metrics.ts'

test('createCoordinatorMetrics returns null when prometheus is unavailable', () => {
  const result = createCoordinatorMetrics()
  strictEqual(result, null)
})

test('createCoordinatorMetrics auto-detects from globalThis.platformatic', () => {
  const registered: any[] = []

  class FakeCounter {
    name: string
    constructor (opts: any) {
      this.name = opts.name
      registered.push(this)
    }

    inc () {}
  }

  class FakeGauge {
    name: string
    constructor (opts: any) {
      this.name = opts.name
      registered.push(this)
    }

    set () {}
  }

  ;(globalThis as any).platformatic = {
    prometheus: {
      client: { Counter: FakeCounter, Gauge: FakeGauge },
      registry: {}
    }
  }

  try {
    const metrics = createCoordinatorMetrics()
    ok(metrics)
    ok(metrics.requestsTotal)
    ok(metrics.podCount)

    const names = registered.map(m => m.name)
    ok(names.includes('coordinator_requests_total'))
    ok(names.includes('coordinator_pod_count'))
  } finally {
    delete (globalThis as any).platformatic
  }
})

test('createCoordinatorMetrics applies the namespace option', () => {
  const registered: any[] = []

  class FakeCounter {
    name: string
    constructor (opts: any) {
      this.name = opts.name
      registered.push(this)
    }

    inc () {}
  }

  class FakeGauge {
    name: string
    constructor (opts: any) {
      this.name = opts.name
      registered.push(this)
    }

    set () {}
  }

  const prometheus = {
    client: { Counter: FakeCounter, Gauge: FakeGauge },
    registry: {}
  }

  const metrics = createCoordinatorMetrics(prometheus, { namespace: 'regina_coordinator' })
  ok(metrics)

  const names = registered.map(m => m.name)
  ok(names.includes('regina_coordinator_requests_total'))
  ok(names.includes('regina_coordinator_pod_count'))
})
