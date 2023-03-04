package collections

import "errors"

// A node in a linked list, has a value and a pointer to the next node.
type Node[T any] struct {
	value T
	next  *Node[T]
	prev  *Node[T]
}

type LinkedList[T any] struct {
	first *Node[T]
}

func NewLinkedList[T any]() LinkedList[T] {
	return LinkedList[T]{}
}

func (n Node[T]) Next() *Node[T] {
	return n.next
}

func (n Node[T]) Prev() *Node[T] {
	return n.prev
}

func (n Node[T]) Value() T {
	return n.value
}

func (l *LinkedList[T]) First() *Node[T] {
	return l.first
}

func (l *LinkedList[T]) Insert(index int, value T) error {
	var prev *Node[T] = nil
	node := l.first
	for i := 0; i < index; i++ {
		if node == nil {
			return errors.New("invalid index")
		}
		prev = node
		node = node.Next()
	}

	newNode := &Node[T]{
		value: value,
		next:  node,
		prev:  prev,
	}

	if node != nil {
		node.prev = newNode
	}

	if prev == nil {
		l.first = newNode
	} else {
		prev.next = newNode
	}

	return nil
}

func (l *LinkedList[T]) InsertAfter(node *Node[T], value T) {
	newNode := &Node[T]{
		value: value,
		next:  node.next,
		prev:  node,
	}
	node.next = newNode
	if newNode.next != nil {
		newNode.next.prev = newNode
	}
}

func (l *LinkedList[T]) InsertBefore(node *Node[T], value T) {
	newNode := &Node[T]{
		value: value,
		next:  node,
		prev:  node.prev,
	}
	if node.prev != nil {
		node.prev.next = newNode
	} else {
		l.first = newNode
	}
	node.prev = newNode
}

func (l *LinkedList[T]) Remove(node *Node[T]) {
	if node.next != nil {
		node.next.prev = node.prev
	}

	if node.prev != nil {
		node.prev.next = node.next
	} else {
		l.first = node.Next()
	}
}

func (l *LinkedList[T]) toArray() []T {
	a := make([]T, 0)

	for node := l.First(); node != nil; node = node.Next() {
		a = append(a, node.Value())
	}

	return a
}
