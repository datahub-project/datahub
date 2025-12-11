/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

export enum SourceCapability {
    PLATFORM_INSTANCE = 'Platform Instance',
    DOMAINS = 'Domains',
    DATA_PROFILING = 'Data Profiling',
    USAGE_STATS = 'Usage Stats',
    PARTITION_SUPPORT = 'Partition Support',
    DESCRIPTIONS = 'Descriptions',
    LINEAGE_COARSE = 'Table-Level Lineage',
    LINEAGE_FINE = 'Column-level Lineage',
    OWNERSHIP = 'Extract Ownership',
    DELETION_DETECTION = 'Detect Deleted Entities',
    TAGS = 'Extract Tags',
    SCHEMA_METADATA = 'Schema Metadata',
    CONTAINERS = 'Asset Containers',
    CLASSIFICATION = 'Classification',
}

export interface ConnectionCapability {
    capable: boolean;
    failure_reason: string | null;
    mitigation_message: string | null;
}

export interface CapabilityReport {
    [key: string]: ConnectionCapability;
}

export interface TestConnectionResult {
    internal_failure?: boolean;
    internal_failure_reason?: string;
    basic_connectivity?: ConnectionCapability;
    capability_report?: CapabilityReport;
}
