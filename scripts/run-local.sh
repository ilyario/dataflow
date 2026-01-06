#!/bin/bash

set -e

echo "Checking for kind or minikube..."

# Check for kind
if command -v kind &> /dev/null; then
    echo "Found kind"
    CLUSTER_TYPE="kind"
elif command -v minikube &> /dev/null; then
    echo "Found minikube"
    CLUSTER_TYPE="minikube"
else
    echo "Warning: Neither kind nor minikube found. Operator will try to connect to current kubeconfig context."
    CLUSTER_TYPE="none"
fi

# Install CRDs
echo "Installing CRDs..."
make install

# Run the operator
echo "Starting operator..."
make run



