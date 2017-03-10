plasma_dump (v0.1.0)
==================

plasma_dump is a diagnostic command line tool, that is designed to assist
in browsing contents of, acquiring stats from, and otherwise diagnosing
plasma log segment stores.

Standalone: How to use
----------------------

    $ go get github.com/couchbase/nitro
    $ cd $GOPATH/src/github.com/couchbase/nitro/cmd/plasma_dump
    $ go build
    $ ./plasma_dump --help

Usage:
------

    plasma_dump <command> [sub-command] [flags] <store_path(s)>

The store_path(s) is one or more directories where plasma files reside.

The command is requred. Available commands:

    dump              Dumps key/val data from the store
    stats             Emits store related stats
    version           Emits the current version of plasma_dump

Use "plasma_dump <command> --help" for more detailed information about
any command.

"dump"
------

    plasma_dump dump [sub-command] [flags] <store_path(s)>

    Available flags:

            --hex       Dumps just data in hex
Examples:

    plasma_dump dump path/to/myStore
    plasma_dump dump path/to/myStore --hex


"stats"
-------

    plasma_dump stats <sub-command> <store_path(s)>

Examples:

    plasma_dump stats path/to/myStore
