-- SPDX-License-Identifier: Apache-2.0
--
-- This file is unmodified from its original version developed by Acryl Data, Inc.,
-- and is now included as part of a repository maintained by the National Digital Twin Programme.
-- All support, maintenance and further development of this code is now the responsibility
-- of the National Digital Twin Programme.

-- Create CDC user with necessary privileges for Debezium
CREATE USER IF NOT EXISTS 'CDC_USER'@'%' IDENTIFIED BY 'CDC_PASSWORD';

-- Grant necessary privileges for CDC operations
GRANT SELECT ON DATAHUB_DB_NAME.* TO 'CDC_USER'@'%';
GRANT RELOAD ON *.* TO 'CDC_USER'@'%';
GRANT REPLICATION CLIENT ON *.* TO 'CDC_USER'@'%';
GRANT REPLICATION SLAVE ON *.* TO 'CDC_USER'@'%';

FLUSH PRIVILEGES;