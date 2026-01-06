#!/bin/bash

set -e

CLUSTER_NAME="dataflow-test"

echo "Setting up kind cluster: $CLUSTER_NAME"

# Check if kind is installed
if ! command -v kind &> /dev/null; then
    echo "Error: kind is not installed. Please install it from https://kind.sigs.k8s.io/"
    exit 1
fi

# Check if cluster already exists
if kind get clusters | grep -q "^${CLUSTER_NAME}$"; then
    echo "Cluster $CLUSTER_NAME already exists. Deleting..."
    kind delete cluster --name $CLUSTER_NAME
fi

# Create cluster
echo "Creating kind cluster..."
kind create cluster --name $CLUSTER_NAME

# Wait for cluster to be ready
echo "Waiting for cluster to be ready..."
kubectl wait --for=condition=Ready nodes --all --timeout=300s --context kind-$CLUSTER_NAME

# Install CRDs
echo "Installing CRDs..."
make install

# Set context
kubectl config use-context kind-$CLUSTER_NAME

echo "Kind cluster $CLUSTER_NAME is ready!"
echo "To use this cluster: kubectl config use-context kind-$CLUSTER_NAME"



