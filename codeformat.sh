#!/bin/bash

# Format source code

set -euxo pipefail

DIR=$(cd "$(dirname "$0")"; pwd -P)

astyle -n --style=ansi  $DIR/reader/src/*.h $DIR/reader/src/*.cc
