import { CorpUser, Entity, SchemaFieldEntity } from '../../../../../../types.generated';

export type QueryBuilderState = {
    urn?: string;
    query: string;
    title?: string;
    description?: string;
};

export type Query = {
    urn?: string;
    query: string;
    title?: string;
    description?: string;
    lastRun?: number;
    createdTime?: number;
    createdBy?: CorpUser | null;
    poweredEntity?: Entity;
    usedBy?: CorpUser[];
    runsPercentileLast30days?: number | null;
    columns?: SchemaFieldEntity[];
};

export enum QueriesTabSection {
    Highlighted,
    Popular,
    Recent,
    Downstream,
}
