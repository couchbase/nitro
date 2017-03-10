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
	"encoding/hex"
	"fmt"

	"github.com/couchbase/nitro/plasma"
	"github.com/spf13/cobra"
)

// dumpCmd represents the dump command
var dumpCmd = &cobra.Command{
	Use:   "dump",
	Short: "Dumps all keys in the specified store",
	Long: `Dumps every key-value persisted in the store in JSON
format.
For example:
	./plasma_dump dump <path_to_store> --hex`,

	PreRunE: func(cmd *cobra.Command, args []string) error {
		if len(args) < 1 {
			return fmt.Errorf("At least one path is required!")
		}
		return nil
	},

	RunE: func(cmd *cobra.Command, args []string) error {
		return invokeDump(args)
	},
}

var inHex bool

func invokeDump(dirs []string) error {
	fmt.Printf("[")
	for index, dir := range dirs {
		cfg := plasma.DefaultConfig()
		cfg.File = dir
		cfg.NumPersistorThreads = 0
		cfg.NumEvictorThreads = 0
		store, err := plasma.New(cfg)
		if err != nil || store == nil {
			return fmt.Errorf("Unable to open store %s, err = %v",
				cfg.File, err)
		}
		defer store.Close()

		iter := store.NewSnapshot().NewIterator()
		defer iter.Close()

		if index != 0 {
			fmt.Printf(",")
		}

		fmt.Printf("{\"%s\":", dir)

		fmt.Printf("[")
		for iter.SeekFirst(); iter.Valid(); iter.Next() {
			k := iter.Key()
			v := iter.Value()
			if inHex {
				fmt.Printf("{\"k\":\"%s\",\"v\":\"%s\"}",
					hex.EncodeToString(k), hex.EncodeToString(v))
			} else {
				fmt.Printf("{\"k\":\"%s\",\"v\":\"%s\"}",
					string(k), string(v))
			}
			fmt.Printf(",\n")
		}
		fmt.Printf("]")

		fmt.Printf("}")
	}
	fmt.Printf("]\n")

	return nil
}

func init() {
	RootCmd.AddCommand(dumpCmd)

	// Local flags that are intended to work as a filter over dump
	dumpCmd.Flags().BoolVar(&inHex, "hex", false,
		"Emits output in hex")
}
