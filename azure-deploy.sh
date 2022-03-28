#!/bin/bash

# Strict mode, fail on any error
set -euo pipefail

# Azure configuration
FILE=".env"
if [[ -f $FILE ]]; then
	echo "loading from .env" | tee -a log.txt
    export $(egrep . $FILE | xargs -n1)
else
	cat << EOF > .env
RESOURCE_GROUP=""
AZURE_STORAGE_ACCOUNT=""
EOF
	echo "Enviroment file not detected."
	echo "Please configure values for your environment in the created .env file"
	echo "and run the script again."
	echo "RESOURCE_GROUP: Resource group where ACI will be deployed (must already exists)"
	echo "AZURE_STORAGE_ACCOUNT: Storage account name that will be created to host the configuration file"
	exit 1
fi

echo "starting"
cat << EOF > log.txt
EOF

echo "using resource group: $RESOURCE_GROUP" | tee -a log.txt

echo "creating storage account: $AZURE_STORAGE_ACCOUNT" | tee -a log.txt
az storage account create -n $AZURE_STORAGE_ACCOUNT -g $RESOURCE_GROUP --sku Standard_LRS \
	-o json >> log.txt	
	
echo "retrieving storage connection string" | tee -a log.txt
AZURE_STORAGE_CONNECTION_STRING=$(az storage account show-connection-string --name $AZURE_STORAGE_ACCOUNT -g $RESOURCE_GROUP -o tsv)

echo 'creating file share' | tee -a log.txt
az storage share create -n config --connection-string $AZURE_STORAGE_CONNECTION_STRING \
	-o json >> log.txt

echo 'uploading configuration ' | tee -a log.txt
az storage file upload-batch --destination config --source client/configs --connection-string $AZURE_STORAGE_CONNECTION_STRING --pattern smartbulkcopy*.config.json \
    -o json >> log.txt

echo "deploying container..." | tee -a log.txt
az deployment group create -g $RESOURCE_GROUP \
	--template-file azure-deploy.arm.json \
	--parameters \
		storageAccountName=$AZURE_STORAGE_ACCOUNT \
		fileShareName=config \
	-o json >> log.txt	

echo "done" | tee -a log.txt