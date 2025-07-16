import { CorpUser, IngestionSourcePrivileges } from '@types';

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
    privileges?: IngestionSourcePrivileges | null;
}

export interface ExecutionCancelInfo {
    executionUrn: string;
    sourceUrn: string;
}
