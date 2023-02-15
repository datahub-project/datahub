import FeatureAvailability from '@site/src/components/FeatureAvailability';

# AWS PrivateLink
<FeatureAvailability saasOnly />

If you require a private connection between the provisioned DataHub instance and your own existing AWS account, Acryl supports using AWS PrivateLink in order to complete this private connection.

In order to complete this connection, the Acryl integrations team will require the AWS ARN for a user or role that can accept and complete the connection to your AWS account. 

Once that team reports the PrivateLink has been created, the team will give you a VPC Endpoint Service Name to use.

In order to complete the connection, you will have to create a VPC Endpoint in your AWS account.  To do so, please follow these instructions:

:::info
Before following the instructions below, please create a VPC security group with ports 80, and 443 (Both TCP) and any required CIDR blocks or other sources as an inbound rule
:::

1. Open the AWS console to the region that the VPC Endpoint Service is created (Generally this will be in `us-west-2 (Oregon)` but will be seen in the service name itself)
2. Browse to the **VPC** Service and click on **Endpoints**
3. Click on **Create Endpoint** in the top right corner
4. Give the endpoint a name tag (such as _datahub-pl_)
5. Click on the **Other endpoint services** radio button
6. In the **Service setting**, copy the service name that was given to you by the integrations team into the **Service name** field and click **Verify Service**
7. Now select the VPC from the dropdown menu where the endpoint will be created.
8. A list of availability zones will now be shown in the **Subnets** section. Please select at least 1 availability zone and then a corresponding subnet ID from the drop down menu to the right of that AZ.
9. Choose **IPv4** for the **IP address type**
10. Choose an existing security group (or multiple) to use on this endpoint
11. (Optional) For **Policy,** you can keep it on **Full access** or **custom** if you have specific access requirements
12. (Optional) Create any tags you wish to add to this endpoint
13. Click **Create endpoint**
14. Once it has been created, Acryl will need to accept the incoming connection from your AWS account; the integrations team will advise you when this has been completed.
