# Configuring Database Retention

## Goal

DataHub uses a database (or key-value store) to store different versions of the aspects as they get ingested. Storing
multiple versions of the aspects allows us to look at the history of how the aspect changed and to rollback to previous
version when incorrect metadata gets ingested. However, each version takes up space in the database, while bringing less
value to the system. We need to be able to impose **retention** on these records to keep the size of the DB in check.

Goal of the retention system is to be able to **configure and enforce retention policies** on documents in various levels (
global, entity-level, aspect-level)

## How to configure?

