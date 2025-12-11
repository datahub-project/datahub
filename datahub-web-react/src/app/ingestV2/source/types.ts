/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Owner } from '@types';

export interface IngestionSourceTableData {
    urn: string;
    type: string;
    name: string;
    platformUrn?: string;
    schedule?: string;
    timezone?: string | null;
    execCount?: number | null;
    lastExecUrn?: string;
    lastExecTime?: number | null;
    lastExecStatus?: string | null;
    cliIngestion: boolean;
    owners?: Owner[] | null;
}
