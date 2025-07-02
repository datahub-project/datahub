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
