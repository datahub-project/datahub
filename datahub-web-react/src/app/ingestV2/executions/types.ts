/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { CorpUser } from '@types';

export interface ExecutionRequestRecord {
    urn: string;
    name?: string;
    type?: string;
    actor?: CorpUser | null;
    id: string;
    // type of source
    source?: string | null;
    sourceUrn?: string | null;
    startedAt?: number | null;
    duration?: number | null;
    status?: string | null;
    showRollback: boolean;
    cliIngestion: boolean;
}

export interface ExecutionCancelInfo {
    executionUrn: string;
    sourceUrn: string;
}

export const enum TabType {
    Summary = 'Summary',
    Logs = 'Logs',
    Recipe = 'Recipe',
}
