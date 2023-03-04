package collections

import (
	"math/rand"
	"sync"

	"golang.org/x/exp/constraints"
)

type SkiplistElement[TKey constraints.Ordered, TValue any] struct {
	key   *TKey
	value *TValue
	next  []*SkiplistElement[TKey, TValue]
}

func (e SkiplistElement[TKey, TValue]) Next() *SkiplistElement[TKey, TValue] {
	return e.next[0]
}

func (e SkiplistElement[TKey, TValue]) Value() (*TKey, *TValue) {
	return e.key, e.value
}

// A SkipList is a probabilistic data structure that offers efficient
// inserts and lookups for keys. Keys are stored in ascending order
// and the elements can be iterated over.
type SkipList[TKey constraints.Ordered, TValue any] struct {
	head             SkiplistElement[TKey, TValue]
	numLayers        int
	layerProbability float32
	numEntries       int
	lock             *sync.RWMutex
}

func NewSkipList[TKey constraints.Ordered, TValue any](numLayers int) SkipList[TKey, TValue] {
	next := make([]*SkiplistElement[TKey, TValue], numLayers)
	head := SkiplistElement[TKey, TValue]{
		key:   nil,
		value: nil,
		next:  next,
	}
	return SkipList[TKey, TValue]{
		head:             head,
		numLayers:        numLayers,
		layerProbability: 0.5,
		numEntries:       0,
		lock:             &sync.RWMutex{},
	}
}

func (l SkipList[TKey, TValue]) Get(key TKey) (*TValue, bool) {
	l.lock.RLock()
	defer l.lock.RUnlock()

	node := &l.head
	for i := l.numLayers - 1; i >= 0; i-- {
		for node.next[i] != nil && *node.next[i].key < key {
			node = node.next[i]
		}
	}

	final := node.next[0]
	if final != nil && final.key != nil && *final.key == key {
		return final.value, true
	} else {
		return nil, false
	}
}

func (l SkipList[TKey, TValue]) randomNumLevels() int {
	levels := 1

	for rand.Float32() < l.layerProbability && levels < l.numLayers {
		levels += 1
	}

	return levels
}

// Inserts an item into the SkipList, or updates an existing item if the key already
// exists. Returns the old value in case of an update.
func (l *SkipList[TKey, TValue]) Insert(key TKey, value TValue) *TValue {
	l.lock.Lock()
	defer l.lock.Unlock()

	update := make([]*SkiplistElement[TKey, TValue], len(l.head.next))
	node := &l.head
	for i := l.numLayers - 1; i >= 0; i-- {
		for node.next[i] != nil && *node.next[i].key < key {
			node = node.next[i]
		}

		update[i] = node
	}

	final := node.next[0]
	if final != nil && final.key != nil && *final.key == key {
		oldValue := node.value
		node.value = &value
		return oldValue
	} else {
		numLevels := l.randomNumLevels()
		newNode := &SkiplistElement[TKey, TValue]{
			key:   &key,
			value: &value,
			next:  make([]*SkiplistElement[TKey, TValue], numLevels),
		}
		for i := 0; i < numLevels; i++ {
			newNode.next[i] = update[i].next[i]
			update[i].next[i] = newNode
		}
		l.numEntries += 1
		return nil
	}
}

func (l SkipList[TKey, TValue]) Iterate() *SkiplistElement[TKey, TValue] {
	return l.head.next[0]
}

func (l SkipList[TKey, TValue]) Len() int {
	return l.numEntries
}
