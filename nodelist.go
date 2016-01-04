package memdb

import (
	"bytes"
	"github.com/t3rm1n4l/memdb/skiplist"
)

type NodeList struct {
	head *skiplist.Node
}

func NewNodeList(head *skiplist.Node) *NodeList {
	return &NodeList{
		head: head,
	}
}

func (l *NodeList) Keys() (keys [][]byte) {
	node := l.head
	for node != nil {
		key := (*Item)(node.Item()).Bytes()
		keys = append(keys, key)
		node = node.GetLink()
	}

	return
}

func (l *NodeList) Remove(key []byte) *skiplist.Node {
	var prev *skiplist.Node
	node := l.head
	for node != nil {
		nodeKey := (*Item)(node.Item()).Bytes()
		if bytes.Equal(nodeKey, key) {
			if prev == nil {
				l.head = node.GetLink()
				return node
			} else {
				prev.SetLink(node.GetLink())
				return node
			}
		}
		prev = node
		node = node.GetLink()
	}

	return nil
}

func (l *NodeList) Add(node *skiplist.Node) {
	node.SetLink(l.head)
	l.head = node
}

func (l *NodeList) Head() *skiplist.Node {
	return l.head
}
