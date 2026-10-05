#!/bin/bash
# ============================================================
#  Cleanup — Delete everything to stop billing
#  Run this when you're done testing!
# ============================================================

echo ""
echo "⚠️  This will DELETE your entire cluster and container images."
echo ""
read -p "Are you sure? (yes/no): " CONFIRM

if [ "$CONFIRM" != "yes" ]; then
    echo "Cancelled."
    exit 0
fi

PROJECT_ID=$(gcloud config get-value project 2>/dev/null)
ZONE="us-central1-a"
CLUSTER_NAME="kafka-pipeline-cluster"

echo ""
echo "🗑️  Deleting Kubernetes resources..."
kubectl delete -f ~/pipeline/k8s/ 2>/dev/null || true

echo "🗑️  Deleting GKE cluster '$CLUSTER_NAME'..."
gcloud container clusters delete $CLUSTER_NAME --zone $ZONE --quiet

echo "🗑️  Deleting container images..."
gcloud container images delete gcr.io/$PROJECT_ID/producer:latest --quiet 2>/dev/null || true
gcloud container images delete gcr.io/$PROJECT_ID/consumer:latest --quiet 2>/dev/null || true

echo ""
echo "✅ Everything deleted. No more charges."
echo ""
