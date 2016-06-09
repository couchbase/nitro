// Copyright (c) 2016 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package nitro

import (
	"bytes"
	"github.com/t3rm1n4l/nitro/skiplist"
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
