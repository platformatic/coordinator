export interface MemberWithLoad {
  memberId: string
  address: string
  resourceCount: number
}

export interface AllocationStrategy {
  pick (members: MemberWithLoad[]): MemberWithLoad | null
}

export class RoundRobinStrategy implements AllocationStrategy {
  #index = 0

  pick (members: MemberWithLoad[]): MemberWithLoad | null {
    if (members.length === 0) return null
    const member = members[this.#index % members.length]
    this.#index = (this.#index + 1) % members.length
    return member
  }
}

export class LeastLoadedStrategy implements AllocationStrategy {
  #tieBreaker = 0

  pick (members: MemberWithLoad[]): MemberWithLoad | null {
    if (members.length === 0) return null
    const minCount = Math.min(...members.map(m => m.resourceCount))
    const candidates = members.filter(m => m.resourceCount === minCount)
    const member = candidates[this.#tieBreaker % candidates.length]
    this.#tieBreaker = (this.#tieBreaker + 1) % candidates.length
    return member
  }
}

export class RandomStrategy implements AllocationStrategy {
  pick (members: MemberWithLoad[]): MemberWithLoad | null {
    if (members.length === 0) return null
    return members[Math.floor(Math.random() * members.length)]
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
