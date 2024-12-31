### Setup

This integration pulls metadata from databases via JDBC connections. It supports various database systems through their respective JDBC drivers.

You'll need:
1. A running database instance that you want to connect to
2. The appropriate JDBC driver for your database
3. Valid credentials with permissions to read metadata

#### Steps to Get Started

1. **JDBC Driver Setup**:
   - Option 1: Download the JDBC driver JAR file for your database
   - Option 2: Use Maven coordinates to automatically download the driver

2. **Permissions Required**:
   - READ access to system catalogs/metadata views
   - Ability to execute metadata queries
   - Access to relevant schemas and tables

3. **Connection Information**:
   - JDBC connection URL
   - Username and password (if using basic authentication)
   - SSL configuration (if required)
   - Any additional JDBC properties needed for your specific database