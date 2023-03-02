#!/bin/bash

# Run parquet2csv container in interactive mode inside k8s
# Local source code is mounted inside the container

# @author  Fabrice Jammes

set -euo pipefail

DIR=$(cd "$(dirname "$0")"; pwd -P)
. "$DIR/conf.sh"

usage() {
    cat << EOD
Usage: $(basename "$0") [options]
Available options:
  -h            This message

Run parquet2csv container in development mode inside k8s

EOD
}

# Get the options
while getopts h c ; do
    case $c in
        h) usage ; exit 0 ;;
        \?) usage ; exit 2 ;;
    esac
done
shift "$((OPTIND-1))"

if [ $# -ne 0 ] ; then
    usage
    exit 2
fi

if false
then
  TELEPRESENCE_VERSION="v2"
  if telepresence version | grep "Client: $TELEPRESENCE_VERSION\." > /dev/null
  then
    echo "Check telepresence version==$TELEPRESENCE_VERSION"
  else
    >&2 echo "ERROR: telepresence v2 is required"
    exit 3
  fi

  if ! curl -ik https://kubernetes.default
  then
    echo "No network access to Kubernetes cluster, re-initializing telepresence"
    telepresence quit
    telepresence connect
  fi
  NAMESPACE=$(kubectl get sa -o=jsonpath='{.items[0]..metadata.namespace}')
fi
$DIR/build.sh

echo "Running in development mode"
MOUNTS="-v $DIR:/opt/parquet2csv"
MOUNTS="$MOUNTS --volume $DIR:$HOME"

CONTAINER="parquet2csv"
echo "oOoOoOoOoOoOoOoOoOoOoOoOoOoOoOoOoOoOoOoOoOoOoOoOoOoO"
echo "   Welcome in $CONTAINER developement container"
echo "oOoOoOoOoOoOoOoOoOoOoOoOoOoOoOoOoOoOoOoOoOoOoOoOoOoO"
echo "Example commands:"
echo "export FINK_DATA_SIM=$HOME/datasim/"
echo "fink_simulator -c $HOME/manifests/base/configmap/fink_alert_simulator.conf"
# docker run --net=host --name "$CONTAINER" --dns-search $NAMESPACE -it $MOUNTS --rm -w "$HOME" "$DEV_IMAGE" bash

docker run --net=host --name "$CONTAINER" -it $MOUNTS --rm -w "$HOME" "$IMAGE" bash
