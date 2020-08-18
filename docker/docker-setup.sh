#!/bin/bash

# Set the version environment variable for the docker build
# Version number is from the VERSION file
export SEARCH_API_VERSION=`cat VERSION`

echo "SEARCH_API_VERSION: $SEARCH_API_VERSION"

# Copy over the src folder
cp -r ../src search-api/


