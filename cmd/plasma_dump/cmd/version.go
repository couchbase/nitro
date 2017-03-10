// Copyright © 2017 Couchbase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cmd

import (
	"fmt"

	"github.com/couchbase/nitro/plasma"
	"github.com/spf13/cobra"
)

// versionCmd represents the version command
var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Retrieves the current version of plasma_dump",
	Run: func(cmd *cobra.Command, args []string) {
		emitVersion()
	},
}

func emitVersion() {
	fmt.Printf("plasma_dump v%s (plasma lib version: %v)\n",
		version, plasma.GetLogVersion())
}

func init() {
	RootCmd.AddCommand(versionCmd)
}
