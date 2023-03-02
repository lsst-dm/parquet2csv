# Build parameters
# ----------------
readonly REPO="gitlab-registry.in2p3.fr/qserv/parquet2csv/parquet2csv"
# Tag to apply to the built image, or to identify the image to be pushed
GIT_HASH="$(git -C $DIR describe --dirty --always)"
readonly IMAGE_TAG="$GIT_HASH"
# WARNING "spark-py" is hard-coded in spark build script
readonly IMAGE="$REPO/parquet2csv:$IMAGE_TAG"

# TODO: use this variable in Dockerfile
ARROW_IMAGE=gitlab-registry.in2p3.fr/qserv/parquet2csv/arrow:11.0.0-1
