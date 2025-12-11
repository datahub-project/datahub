/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

/**
 * Steps of the Ingestion Source Builder flow.
 */
export enum IngestionSourceBuilderStep {
    SELECT_TEMPLATE = 'SELECT_TEMPLATE',
    DEFINE_RECIPE = 'DEFINE_RECIPE',
    CREATE_SCHEDULE = 'CREATE_SCHEDULE',
    NAME_SOURCE = 'NAME_SOURCE',
}
