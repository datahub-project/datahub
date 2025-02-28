---
title: "Deploying to Azure"
---

# Azure setup guide

The following is a set of instructions to quickstart DataHub on Azure Kubernetes Service (AKS). Note, the guide
assumes that you do not have a Kubernetes cluster set up. 

## Prerequisites

This guide requires the following tools:

- [kubectl](https://kubernetes.io/docs/tasks/tools/) to manage Kubernetes resources
- [helm](https://helm.sh/docs/intro/install/) to deploy the resources based on helm charts. Note, we only support Helm
    3.
- [AZ CLI](https://learn.microsoft.com/en-us/cli/azure/install-azure-cli) to manage Azure resources

To use the above tools, you need to set up Azure credentials by following
this [guide](https://learn.microsoft.com/en-us/cli/azure/authenticate-azure-cli).

## Start up a Kubernetes cluster on AKS

You can follow this [guide](https://learn.microsoft.com/en-us/azure/aks/learn/quick-kubernetes-deploy-cli) to create a new
cluster using az cli. 

Note: you can skip the application deployment step since we are deploying DataHub instead. If you are deploying DataHub to an existing cluster, please
skip the corresponding sections.

- Verify you have the Microsoft.OperationsManagement and Microsoft.OperationalInsights providers registered on your subscription. These Azure resource providers are required to support Container insights. Check the registration status using the following commands:

```
az provider show -n Microsoft.OperationsManagement -o table
az provider show -n Microsoft.OperationalInsights -o table
```

If they're not registered, register them using the following commands:

```
az provider register --namespace Microsoft.OperationsManagement
az provider register --namespace Microsoft.OperationalInsights
```

- Create a resource group. Change name, location to your choosing.

```
az group create --name myResourceGroup --location eastus
```

The following output indicates that the command execution was successful:

```
{
  "id": "/subscriptions/<guid>/resourceGroups/myResourceGroup",
  "location": "eastus",
  "managedBy": null,
  "name": "myResourceGroup",
  "properties": {
    "provisioningState": "Succeeded"
  },
  "tags": null
}
```
- Create an AKS Cluster. For this project, it is best to increase node count to at least 3. Change cluster name, node count, and addons to your choosing.

```
az aks create -g myResourceGroup -n myAKSCluster --enable-managed-identity --node-count 3 --enable-addons monitoring --generate-ssh-keys
```

After a few minutes, the command completes and returns JSON-formatted information about the cluster.

- Connect to the cluster

Configure kubectl to connect to your Kubernetes cluster using the az aks get-credentials command.

```
az aks get-credentials --resource-group myResourceGroup --name myAKSCluster
```

Verify the connection to your cluster using the `kubectl get` command. This command returns a list of the cluster nodes.

```
kubectl get nodes
```

You should get results like below. Make sure node status is Ready.

```
NAME                                          STATUS   ROLES    AGE   VERSION
aks-nodepool1-37660971-vmss000000              Ready    agent   24h   v1.25.6
aks-nodepool1-37660971-vmss000001              Ready    agent   24h   v1.25.6
aks-nodepool1-37660971-vmss000002              Ready    agent   24h   v1.25.6
```

## Setup DataHub using Helm

Once the Kubernetes cluster has been set up, you can deploy DataHub and its prerequisites using helm. Please follow the
steps in this [guide](kubernetes.md). 


Notes:
Since we are using PostgreSQL as the storage layer, change postgresql enabled to true and mysql to false in the values.yaml file of prerequisites.
Additionally, create a postgresql secret. Make sure to include 3 passwords for the postgresql secret: postgres-password, replication-password, and password.

## Expose endpoints using a load balancer

Now that all the pods are up and running, you need to expose the datahub-frontend end point by setting
up [ingress](https://kubernetes.io/docs/concepts/services-networking/ingress/). To do this, you need to first set up an
ingress controller. 


There are many [ingress controllers](https://kubernetes.io/docs/concepts/services-networking/ingress-controllers/)  to choose
from, but here, we will follow this [guide](https://learn.microsoft.com/en-us/azure/application-gateway/tutorial-ingress-controller-add-on-existing) to set up the Azure
Application Gateway Ingress Controller. 

- Deploy a New Application Gateway.

First, you need to create a WAF policy

```
az network application-gateway waf-policy create -g myResourceGroup -n myWAFPolicy
```

- Before the application gateway can be deployed, you'll also need to create a public IP resource, a new virtual network with address space 10.0.0.0/16, and a subnet with address space 10.0.0.0/24. 
Then, you can deploy your application gateway in the subnet using the publicIP.

Caution: When you use an AKS cluster and application gateway in separate virtual networks, the address spaces of the two virtual networks must not overlap. The default address space that an AKS cluster deploys in is 10.224.0.0/12.


```
az network public-ip create -n myPublicIp -g myResourceGroup --allocation-method Static --sku Standard
az network vnet create -n myVnet -g myResourceGroup --address-prefix 10.0.0.0/16 --subnet-name mySubnet --subnet-prefix 10.0.0.0/24 
az network application-gateway create -n myApplicationGateway -l eastus -g myResourceGroup --sku WAF_v2 --public-ip-address myPublicIp --vnet-name myVnet --subnet mySubnet --priority 100 --waf-policy /subscriptions/{subscription_id}/resourceGroups/myResourceGroup/providers/Microsoft.Network/ApplicationGatewayWebApplicationFirewallPolicies/myWAFPolicy
```
Change myPublicIp, myResourceGroup, myVnet, mySubnet, and myApplicationGateway to names of your choosing.


- Enable the AGIC Add-On in Existing AKS Cluster Through Azure CLI

```
appgwId=$(az network application-gateway show -n myApplicationGateway -g myResourceGroup -o tsv --query "id") 
az aks enable-addons -n myCluster -g myResourceGroup -a ingress-appgw --appgw-id $appgwId
```

- Peer the Two Virtual Networks Together

Since you deployed the AKS cluster in its own virtual network and the Application gateway in another virtual network, you'll need to peer the two virtual networks together in order for traffic to flow from the Application gateway to the pods in the cluster.

```
nodeResourceGroup=$(az aks show -n myCluster -g myResourceGroup -o tsv --query "nodeResourceGroup")
aksVnetName=$(az network vnet list -g $nodeResourceGroup -o tsv --query "[0].name")

aksVnetId=$(az network vnet show -n $aksVnetName -g $nodeResourceGroup -o tsv --query "id")
az network vnet peering create -n AppGWtoAKSVnetPeering -g myResourceGroup --vnet-name myVnet --remote-vnet $aksVnetId --allow-vnet-access

appGWVnetId=$(az network vnet show -n myVnet -g myResourceGroup -o tsv --query "id")
az network vnet peering create -n AKStoAppGWVnetPeering -g $nodeResourceGroup --vnet-name $aksVnetName --remote-vnet $appGWVnetId --allow-vnet-access
```

- Deploy the Ingress on the Frontend Pod

In order to use the ingress controller to expose frontend pod, we need to update the datahub-frontend section of the values.yaml file that was used to deploy DataHub. Here is a sample configuration:

```
datahub-frontend:
  enabled: true
  image:
    repository: acryldata/datahub-frontend-react
    # tag: "v0.10.0 # defaults to .global.datahub.version

  # Set up ingress to expose react front-end
  ingress:
    enabled: true
    annotations:
      kubernetes.io/ingress.class: azure/application-gateway
      appgw.ingress.kubernetes.io/backend-protocol: "http" 
    
    hosts:
    - paths:
      - /*
  defaultUserCredentials: {}
```

You can then apply the updates:

```
helm upgrade --install datahub datahub/datahub --values values.yaml
```

You can now verify that the ingress was created correctly

```
kubectl get ingress
```

You should see a result like this:

![frontend-image](https://github.com/Saketh-Mahesh/azure-docs-images/blob/main/frontend-status.png?raw=true)

## Use PostgresSQL for the storage layer
Configure a PostgreSQL database in the same virtual network as the Kubernetes cluster or implement virtual network peering to connect both networks. Once the database is provisioned, you should be able to see the following page under the Connect tab on the left side. 


Note: PostgreSQL Database MUST be deployed in same location as AKS/resource group (eastus, centralus, etc.)
Take a note of the connection details:

![postgres-info](https://github.com/Saketh-Mahesh/azure-docs-images/blob/main/postgres-info.png?raw=true)





- Update the postgresql settings under global in the values.yaml as follows.

```
global:
  sql:
    datasource:
      host: "${POSTGRES_HOST}.postgres.database.azure.com:5432"
      hostForpostgresqlClient: "${POSTGRES_HOST}.postgres.database.azure.com"
      port: "5432"
      url: "jdbc:postgresql://${POSTGRES_HOST}.postgres.database.azure.com:5432/datahub?user=${POSTGRES_ADMIN_LOGIN}&password=${POSTGRES_ADMIN_PASSWORD}&sslmode=require"
      driver: "org.postgresql.Driver"
      username: "${POSTGRES_ADMIN_LOGIN}"
      password:
        value: "${POSTGRES_ADMIN_PASSWORD}"
```
Run this command helm command to update datahub configuration

```
helm upgrade --install datahub datahub/datahub --values values.yaml
```

And there you go! You have now installed DataHub on an Azure Kubernetes Cluster with an ingress controller set up to expose the frontend. Additionally you have utilized PostgreSQL as the storage layer of DataHub.