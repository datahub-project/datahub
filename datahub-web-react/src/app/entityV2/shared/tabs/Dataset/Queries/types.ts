import { CorpUser, Entity } from '../../../../../../types.generated';

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
    executedTime?: number;
    createdTime?: number;
    createdBy?: CorpUser | null;
    poweredEntity?: Entity;
    usedBy?: CorpUser[];
    runsPercentileLast30days?: number | null;
};

export enum QueriesTabSection {
    Highlighted,
    Popular,
    Recent,
    Downstream,
}
