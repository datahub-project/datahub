import {
    Assertion,
    AssertionResultType,
    AssertionRunEvent,
    AssertionRunStatus,
    AssertionSourceType,
    AssertionType,
    AuditStamp,
    DataPlatform,
    EntityType,
    Ownership,
    TagAssociation,
} from '@src/types.generated';

export type EntityStagedForAssertion = {
    urn: string;
    platform: DataPlatform;
    entityType: EntityType;
};

export type AssertionBuilderSiblingOptions = {
    title: string;
    disabled?: boolean;
} & Partial<EntityStagedForAssertion>;

export type AssertionListFilter = {
    sortBy: string;
    groupBy: string;
    filterCriteria: {
        searchText: string;
        status: AssertionResultType[];
        type: AssertionType[];
        tags: string[];
        column: string[];
        source: AssertionSourceType[];
        owners: string[];
    };
};

export type AssertionListTableRow = {
    key: string;
    type?: AssertionType | string;
    lastUpdated?: AuditStamp;
    tags: TagAssociation[];
    descriptionHTML: JSX.Element | null;
    description: string;
    urn: string;
    platform: DataPlatform;
    lastEvaluation?: AssertionRunEvent;
    lastEvaluationTimeMs?: number;
    lastEvaluationResult?: AssertionResultType; // add type
    lastEvaluationUrl?: string;
    assertion: Assertion;
    ownership?: Ownership | null;
    status: AssertionRunStatus; // status;
    groupName?: string;
    name?: string;
};

export type AssertionStatusGroup = {
    name: string;
    assertions: AssertionListTableRow[];
    summary: Record<string, number>;
    groupName?: JSX.Element;
};

export type AssertionColumnGroup = {
    name: string;
    assertions: AssertionListTableRow[];
    summary?: Record<string, number>;
};

export type AssertionTable = {
    assertions: AssertionListTableRow[];
    groupBy: {
        type: any[];
        status: AssertionStatusGroup[];
        column: AssertionColumnGroup[];
    };
    filterOptions?: AssertionFilterOptions;
    originalFilterOptions?: AssertionFilterOptions;
    filteredCount?: number;
    searchMatchesCount?: number;
    totalCount?: number;
};

export type AssertionFilterOptions = {
    filterGroupOptions: {
        type: AssertionRecommendedFilter[];
        status: AssertionRecommendedFilter[];
        column: AssertionRecommendedFilter[];
        tags: AssertionRecommendedFilter[];
        source: AssertionRecommendedFilter[];
        owners: AssertionRecommendedFilter[];
    };
    recommendedFilters: AssertionRecommendedFilter[];
};

export type AssertionRecommendedFilter = {
    name: string;
    category: 'status' | 'type' | 'source' | 'tags' | 'column' | 'owners';
    count: number;
    displayName: string;
};

export type AssertionWithDescription = Assertion & { description: string };
