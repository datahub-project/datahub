import { CorpUser } from '@types';

export interface ExecutionRequestRecord {
    urn: string;
    name?: string;
    type?: string;
    actor?: CorpUser | null;
    id: string;
    // type of source
    source?: string | null;
    startedAt?: number | null;
    duration?: number | null;
    status?: string | null;
    showRollback: boolean;
    cliIngestion: boolean;
}
