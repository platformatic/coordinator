export interface MemberInfo {
  memberId: string
  address: string
  load: number
}

export interface PickContext {
  destinationId?: string
}

export interface AllocationStrategy {
  pick (candidates: MemberInfo[], ctx: PickContext): MemberInfo | null
}

export class RoundRobinStrategy implements AllocationStrategy {
  #index = 0

  pick (candidates: MemberInfo[], _ctx: PickContext): MemberInfo | null {
    if (candidates.length === 0) return null
    const member = candidates[this.#index % candidates.length]
    this.#index = (this.#index + 1) % candidates.length
    return member
  }
}

export class LeastLoadedStrategy implements AllocationStrategy {
  #tieBreaker = 0

  pick (candidates: MemberInfo[], _ctx: PickContext): MemberInfo | null {
    if (candidates.length === 0) return null
    const min = Math.min(...candidates.map(m => m.load))
    const tied = candidates.filter(m => m.load === min)
    const member = tied[this.#tieBreaker % tied.length]
    this.#tieBreaker = (this.#tieBreaker + 1) % tied.length
    return member
  }
}

export class RandomStrategy implements AllocationStrategy {
  pick (candidates: MemberInfo[], _ctx: PickContext): MemberInfo | null {
    if (candidates.length === 0) return null
    return candidates[Math.floor(Math.random() * candidates.length)]
  }
}

export function createStrategy (name: string): AllocationStrategy {
  switch (name) {
    case 'least-loaded':
      return new LeastLoadedStrategy()
    case 'random':
      return new RandomStrategy()
    case 'round-robin':
    default:
      return new RoundRobinStrategy()
  }
}
