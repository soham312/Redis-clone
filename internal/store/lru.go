package store

import "sync"

// lruNode is one entry in the recency doubly linked list. Only the key is
// stored here — the actual value lives in Store.data; this list exists
// purely to track access order so the least-recently-used key can be found
// in O(1) instead of scanning everything.
type lruNode struct {
	key        string
	prev, next *lruNode
}

// lruPolicy is the classic "hash map + doubly linked list" LRU, built by
// hand as requested: the doubly linked list orders keys from most-recently
// used (right after head) to least-recently used (right before tail), and
// the hash map (here, our own HashTable, not Go's builtin map — see
// hashtable.go) gives O(1) lookup from a key straight to its list node, so
// "move this key to the front" never requires walking the list.
//
// Three operations, all O(1):
//   - Touch: look the node up via the hash map, unlink it from wherever it
//     currently sits, and relink it at the front (most-recently-used end).
//   - Evict: the node just before the tail sentinel *is* the
//     least-recently-used key by construction — no search needed.
//   - RemoveKey: same lookup-then-unlink as Touch, minus the relink.
//
// The list uses two sentinel nodes (head, tail) rather than leaving the
// ends as nil. That's a standard trick that removes every "is this the
// first/last real node" branch from unlink/insert: every real node always
// has a non-nil prev and next (worst case, a sentinel), so the linking
// logic is exactly the same whether the list has 0, 1, or a million real
// entries in it.
type lruPolicy struct {
	mu         sync.Mutex
	nodes      *HashTable[*lruNode]
	head, tail *lruNode
}

func newLRUPolicy() *lruPolicy {
	p := &lruPolicy{nodes: NewHashTable[*lruNode]()}
	p.head = &lruNode{}
	p.tail = &lruNode{}
	p.head.next = p.tail
	p.tail.prev = p.head
	return p
}

func (p *lruPolicy) Touch(key string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if node, ok := p.nodes.Get(key); ok {
		p.unlink(node)
		p.pushFront(node)
		return
	}

	node := &lruNode{key: key}
	p.nodes.Set(key, node)
	p.pushFront(node)
}

func (p *lruPolicy) RemoveKey(key string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	node, ok := p.nodes.Get(key)
	if !ok {
		return
	}
	p.unlink(node)
	p.nodes.Delete(key)
}

// Evict removes and returns the least-recently-used key: the node
// immediately before the tail sentinel. If that node IS the head sentinel,
// the list is empty.
func (p *lruPolicy) Evict() (string, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	victim := p.tail.prev
	if victim == p.head {
		return "", false
	}
	p.unlink(victim)
	p.nodes.Delete(victim.key)
	return victim.key, true
}

func (p *lruPolicy) Clear() {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.nodes = NewHashTable[*lruNode]()
	p.head.next = p.tail
	p.tail.prev = p.head
}

// unlink splices n out of the list. Caller must hold p.mu.
func (p *lruPolicy) unlink(n *lruNode) {
	n.prev.next = n.next
	n.next.prev = n.prev
}

// pushFront inserts n as the new most-recently-used node, right after
// head. Caller must hold p.mu, and n must not currently be linked into the
// list.
func (p *lruPolicy) pushFront(n *lruNode) {
	n.next = p.head.next
	n.prev = p.head
	p.head.next.prev = n
	p.head.next = n
}
