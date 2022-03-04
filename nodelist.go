// Copyright 2016-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package nitro

import (
	"bytes"
	"github.com/couchbase/nitro/skiplist"
)

// NodeList is a linked list of skiplist nodes
type NodeList struct {
	head *skiplist.Node
}

// NewNodeList creates new node list
func NewNodeList(head *skiplist.Node) *NodeList {
	return &NodeList{
		head: head,
	}
}

// Keys returns all keys from the node list
func (l *NodeList) Keys() (keys [][]byte) {
	node := l.head
	for node != nil {
		key := (*Item)(node.Item()).Bytes()
		keys = append(keys, key)
		node = node.GetLink()
	}

	return
}

// Remove a key from the node list
func (l *NodeList) Remove(key []byte) *skiplist.Node {
	var prev *skiplist.Node
	node := l.head
	for node != nil {
		nodeKey := (*Item)(node.Item()).Bytes()
		if bytes.Equal(nodeKey, key) {
			if prev == nil {
				l.head = node.GetLink()
				return node
			}

			prev.SetLink(node.GetLink())
			return node
		}
		prev = node
		node = node.GetLink()
	}

	return nil
}

// Add a key into the node list
func (l *NodeList) Add(node *skiplist.Node) {
	node.SetLink(l.head)
	l.head = node
}

// Head returns head node from the list
func (l *NodeList) Head() *skiplist.Node {
	return l.head
}
