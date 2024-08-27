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
};
