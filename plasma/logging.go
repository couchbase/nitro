// Copyright (c) 2017 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package plasma

import "fmt"

type Logger interface {
	Warnf(format string, v ...interface{})

	Errorf(format string, v ...interface{})

	Fatalf(format string, v ...interface{})

	Infof(format string, v ...interface{})

	Debugf(format string, v ...interface{})

	Tracef(format string, v ...interface{})
}

type defaultLogger struct {
}

func (*defaultLogger) Fatalf(f string, v ...interface{}) {
	fmt.Printf(f, v...)
}

func (*defaultLogger) Errorf(f string, v ...interface{}) {
	fmt.Printf(f, v...)
}

func (*defaultLogger) Warnf(f string, v ...interface{}) {
	fmt.Printf(f, v...)
}

func (*defaultLogger) Infof(f string, v ...interface{}) {
	fmt.Printf(f, v...)
}

func (*defaultLogger) Debugf(f string, v ...interface{}) {
	fmt.Printf(f, v...)
}

func (*defaultLogger) Tracef(f string, v ...interface{}) {
	fmt.Printf(f, v...)
}
