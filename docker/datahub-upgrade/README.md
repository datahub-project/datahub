# DataHub Upgrade Docker Image

This container is used to automatically apply upgrades from one version of DataHub to another.

As of today, there are 2 supported upgrades:

1. No Code Data Migration: Performs a series of pre-flight qualification checks and then migrates metadata_aspect table data
to metadata_aspect_v2 table. 
   
2. No Code Cleanup: Cleanses graph index, search index, and key-value store of legacy DataHub data (metadata_aspect table). 

