### Setup

This integration pulls metadata directly from Cassandra databases, including both **DataStax Astra DB** and **Cassandra Enterprise Edition (EE)**. 

You’ll need to have a Cassandra instance or an Astra DB setup with appropriate access permissions.

#### Steps to Get the Required Information

1. **Set Up User Credentials**:

   - **For Astra DB**:
     - Log in to your Astra DB Console.
     - Navigate to **Organization Settings** > **Token Management**.
     - Generate an **Application Token** with the required permissions for read access.
     - Download the **Secure Connect Bundle** from the Astra DB Console.
   - **For Cassandra EE**:
     - Ensure you have a **username** and **password** with read access to the necessary keyspaces.

2. **Permissions**:

   - The user or token must have `SELECT` permissions that allow it to:
     - Access metadata in system keyspaces (e.g., `system_schema`) to retrieve information about keyspaces, tables, columns, and views.
     - Perform `SELECT` operations on the data tables if data profiling is enabled.

3. **Verify Database Access**:
   - For Astra DB: Ensure the **Secure Connect Bundle** is used and configured correctly.
   - For Cassandra Opensource: Ensure the **contact point** and **port** are accessible.


:::caution 

When enabling profiling, make sure to set a limit on the number of rows to sample. Profiling large tables without a limit may lead to excessive resource consumption and slow performance.

:::

:::note

For cloud configuration with Astra DB, it is necessary to specify the Secure Connect Bundle path in the configuration. For that reason, use the CLI to ingest metadata into DataHub.

:::
