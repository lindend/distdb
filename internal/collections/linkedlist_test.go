package collections

import (
	. "testing"

	"github.com/stretchr/testify/assert"
)

func TestInsertItem(t *T) {
	l := NewLinkedList[int]()
	l.Insert(0, 3)

	assert.Equal(t, 3, l.First().Value(), "Should store value")
	assert.Nil(t, l.First().Next(), "First() should have nil next")
	assert.Nil(t, l.First().Prev(), "First() should have nil prev")
}

func TestInsertTwo(t *T) {
	l := NewLinkedList[int]()
	l.Insert(0, 3)
	l.Insert(1, 4)

	assert.Nil(t, l.First().Prev(), "First().Prev() should not be nil")
	assert.Nil(t, l.First().Next().Next(), "First().Next().Next() should not be nil")
	assert.Equal(t, []int{3, 4}, l.toArray(), "Should match array")
}

func TestInsertAfter(t *T) {
	l := NewLinkedList[int]()
	l.Insert(0, 1)
	l.InsertAfter(l.First(), 2)

	assert.Equal(t, []int{1, 2}, l.toArray(), "Should match array")
}

func TestInsertAfterMiddle(t *T) {
	l := NewLinkedList[int]()
	l.Insert(0, 1)
	l.Insert(1, 2)
	l.InsertAfter(l.First(), 3)

	assert.Equal(t, []int{1, 3, 2}, l.toArray(), "Should match array")
}

func TestInsertBeforeFirst(t *T) {
	l := NewLinkedList[int]()
	l.Insert(0, 1)
	l.InsertBefore(l.First(), 2)

	assert.Nil(t, l.First().Prev(), "First should have nil prev")
	assert.Nil(t, l.First().Next().Next(), "Last should have nil next")
	assert.Equal(t, []int{2, 1}, l.toArray())
}

func TestRemoveOne(t *T) {
	l := NewLinkedList[int]()
	l.Insert(0, 1)
	l.Remove(l.First())

	assert.Nil(t, l.First(), "Removed only element should be empty")
}

func TestRemoveSecond(t *T) {
	l := NewLinkedList[int]()
	l.Insert(0, 1)
	l.Insert(1, 2)
	l.Remove(l.First().Next())

	assert.Nil(t, l.First().Prev(), "First element should have nil prev")
	assert.Nil(t, l.First().Next(), "Only element should have nil next")
	assert.Equal(t, 1, l.First().Value(), "Value should be 1")
}

func TestRemoveMiddle(t *T) {
	l := NewLinkedList[int]()
	l.Insert(0, 1)
	l.Insert(1, 2)
	l.Insert(2, 3)

	assert.Equal(t, []int{1, 2, 3}, l.toArray(), "Sanity check setup")

	l.Remove(l.First().Next())

	assert.Equal(t, 3, l.First().Next().Value(), "Value of second should be 3")
	assert.Nil(t, l.First().Prev(), "First prev should be nil")
	assert.Nil(t, l.First().Next().Next(), "Last next should be nil")
	assert.Equal(t, []int{1, 3}, l.toArray())
}

func TestRemoveFirst(t *T) {
	l := NewLinkedList[int]()
	l.Insert(0, 1)
	l.Insert(1, 2)

	l.Remove(l.First())

	assert.NotNil(t, l.First(), "Should not be empty")
	assert.Nil(t, l.First().Prev(), "Only element should have nil prev")
	assert.Nil(t, l.First().Next(), "Only element should have nil next")
	assert.Equal(t, 2, l.First().Value(), "Should match value")
}

func TestKeepsLinksCorrect(t *T) {
	l := NewLinkedList[int]()

	for i := 0; i < 2; i++ {
		l.Insert(0, 1)
		l.Insert(1, 2)
		l.Insert(0, 4)
		l.InsertBefore(l.First(), 5)
		l.InsertAfter(l.First(), 6)
		l.InsertBefore(l.First().Next().Next(), 7)
		l.InsertAfter(l.First().Next(), 8)
		l.Remove(l.First().Next())
		l.Remove(l.First())
	}

	node := l.First()
	assert.NotNil(t, l.First(), "Should contain elements")

	for node != nil {
		if node.Next() != nil {
			if node.Next().Prev() != node {
				t.Fatalf("Node chain violation detected, Next().Prev() != node on node %d", node.Value())
			}
		}

		if node.Prev() != nil {
			if node.Prev().Next() != node {
				t.Fatalf("Node chain violation detected, Prev().Next() != node")
			}
		}
		node = node.Next()
	}

	assert.Equal(t, []int{8, 7, 4, 1, 2, 8, 7, 4, 1, 2}, l.toArray(), "Should match")
}
