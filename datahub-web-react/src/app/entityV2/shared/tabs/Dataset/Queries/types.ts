import { CorpUser, Entity, SchemaFieldEntity } from '@types';

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
    columns?: SchemaFieldEntity[];
};

export enum QueriesTabSection {
    Highlighted,
    Popular,
    Recent,
    Downstream,
}
