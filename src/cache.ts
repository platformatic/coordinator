export interface CacheOptions {
  ttl?: number  // milliseconds; default 5000
  max?: number  // entries; default 10000
}

interface Entry<V> {
  value: V
  expires: number
}

export class TTLCache<K, V> {
  #map = new Map<K, Entry<V>>()
  #ttl: number
  #max: number

  constructor (opts: CacheOptions = {}) {
    this.#ttl = opts.ttl ?? 5000
    this.#max = opts.max ?? 10000
  }

  get (key: K): V | undefined {
    const entry = this.#map.get(key)
    if (!entry) return undefined
    if (entry.expires < Date.now()) {
      this.#map.delete(key)
      return undefined
    }
    // Refresh insertion order so frequently-read entries are evicted last.
    this.#map.delete(key)
    this.#map.set(key, entry)
    return entry.value
  }

  set (key: K, value: V): void {
    if (this.#map.has(key)) this.#map.delete(key)
    while (this.#map.size >= this.#max) {
      const oldest = this.#map.keys().next().value
      if (oldest === undefined) break
      this.#map.delete(oldest)
    }
    this.#map.set(key, { value, expires: Date.now() + this.#ttl })
  }

  delete (key: K): void {
    this.#map.delete(key)
  }

  clear (): void {
    this.#map.clear()
  }
}
