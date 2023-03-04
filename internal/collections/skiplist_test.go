package collections

import (
	. "testing"

	"github.com/stretchr/testify/assert"
)

func TestAddItem(t *T) {
	sl := NewSkipList[int, int](4)
	sl.Insert(0, 1)

	v, exists := sl.Get(0)
	assert.True(t, exists)
	assert.Equal(t, 1, *v)
}

func TestIteratesInSortedKeyOrder(t *T) {
	sl := NewSkipList[int, int](4)
	sl.Insert(3, 0)
	sl.Insert(2, 3)
	sl.Insert(5, 6)
	sl.Insert(1, 3)
	sl.Insert(4, 7)

	i := sl.Iterate()
	k, _ := i.Value()
	assert.Equal(t, 1, *k)

	i = i.Next()
	k, _ = i.Value()
	assert.Equal(t, 2, *k)

	i = i.Next()
	k, _ = i.Value()
	assert.Equal(t, 3, *k)

	i = i.Next()
	k, _ = i.Value()
	assert.Equal(t, 4, *k)

	i = i.Next()
	k, _ = i.Value()
	assert.Equal(t, 5, *k)
}
