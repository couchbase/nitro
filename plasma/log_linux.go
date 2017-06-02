// Copyright (c) 2017 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package plasma

import (
	"os"
	"sync/atomic"
)

const minHolePunchSize = 512 * 1024 * 1024
const FALLOC_FL_PUNCH_HOLEOC_FL_KEEP_SIZE = 0x01
const FALLOC_FL_PUNCH_HOLE = 0x02

type singleFileLog struct {
	fd                     *os.File
	headOffset, tailOffset int64
	sbBuffer               [logSBSize]byte
	sbGen                  int64
	lastTrimOffset         int64
}

func newSingleFileLog(path string) (Log, error) {
	var sbBuffer [logSBSize]byte

	fd, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		return nil, err
	}

	h, t, g, err := readLogSB(fd, sbBuffer[:])
	if err != nil {
		return nil, err
	}

	log := &singleFileLog{
		fd:         fd,
		headOffset: h,
		tailOffset: t,
		sbGen:      g + 1,
	}

	return log, nil
}

func (l *singleFileLog) Head() int64 {
	return atomic.LoadInt64(&l.headOffset)
}

func (l *singleFileLog) Tail() int64 {
	return atomic.LoadInt64(&l.tailOffset)
}

func (l *singleFileLog) Read(bs []byte, off int64) error {
	_, err := l.fd.ReadAt(bs, off+2*logSBSize)
	return err
}

func (l *singleFileLog) Append(bs []byte) error {
	if _, err := l.fd.WriteAt(bs, l.tailOffset); err != nil {
		return err
	}

	atomic.AddInt64(&l.tailOffset, int64(len(bs)))
	return nil
}

func (l *singleFileLog) Trim(offset int64) {
	l.headOffset = offset
}

func (l *singleFileLog) Commit() error {
	marshalLogSB(l.sbBuffer[:], l.headOffset, l.tailOffset, l.sbGen)
	offset := int64(logSBSize * (l.sbGen % 2))
	if _, err := l.fd.WriteAt(l.sbBuffer[:], offset); err != nil {
		return err
	}

	if err := l.tryHolePunch(); err != nil {
		return err
	}
	l.sbGen++
	return nil
}

func (l *singleFileLog) Size() int64 {
	return atomic.LoadInt64(&l.tailOffset) - atomic.LoadInt64(&l.headOffset)
}

func (l *singleFileLog) Close() error {
	return l.fd.Close()
}

func (l *singleFileLog) tryHolePunch() error {
	free := minHolePunchSize * ((l.headOffset - l.lastTrimOffset) / minHolePunchSize)
	if free > 0 {
		if err := punchHole(l.fd, l.lastTrimOffset, free); err != nil {
			return err
		}
		l.lastTrimOffset += free
	}

	return nil
}
