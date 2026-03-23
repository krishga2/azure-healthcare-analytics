#!/bin/bash
# Azure Healthcare Analytics Platform - Infrastructure Setup
# Run in Azure Cloud Shell

RESOURCE_GROUP="rg-healthcare-analytics"
STORAGE_ACCOUNT="healthcareadls2026"
LOCATION="southcentralus"
ADF_NAME="adf-healthcare-2026"

az group create --name $RESOURCE_GROUP --location $LOCATION

az storage account create \
  --name $STORAGE_ACCOUNT \
  --resource-group $RESOURCE_GROUP \
  --location $LOCATION \
  --sku Standard_LRS \
  --kind StorageV2 \
  --enable-hierarchical-namespace true

az storage fs create --name bronze --account-name $STORAGE_ACCOUNT --auth-mode login
az storage fs create --name silver --account-name $STORAGE_ACCOUNT --auth-mode login
az storage fs create --name gold   --account-name $STORAGE_ACCOUNT --auth-mode login

az datafactory create \
  --name $ADF_NAME \
  --resource-group $RESOURCE_GROUP \
  --location $LOCATION

az provider register --namespace Microsoft.Sql
az provider register --namespace Microsoft.Synapse

az synapse workspace create \
  --name synapse-healthcare-2026 \
  --resource-group $RESOURCE_GROUP \
  --location $LOCATION \
  --storage-account $STORAGE_ACCOUNT \
  --file-system gold \
  --sql-admin-login-user sqladmin \
  --sql-admin-login-password Healthcare@2026!

echo "Infrastructure setup complete"
