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
    TagAssociation,
} from '@src/types.generated';
import { AssertionGroup } from '../acrylTypes';

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
    };
};

export type AssertionListTableRow = {
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
    status: AssertionRunStatus; // status;
    groupName?: string;
    name?: string;
};

export type AssertionGroupExtended = Omit<AssertionGroup, 'assertions'> & {
    assertions: AssertionListTableRow[];
    groupName?: JSX.Element;
};

export type AssertionStatusGroup = {
    name: string;
    assertions: AssertionListTableRow[];
    summary: { [key: string]: number };
    groupName?: JSX.Element;
};

export type AssertionColumnGroup = {
    name: string;
    assertions: AssertionListTableRow[];
    summary?: { [key: string]: number };
};

type AssertionGroupBy = {
    type: any[];
    status: AssertionStatusGroup[];
    column: AssertionColumnGroup[];
};

export type AssertionTable = {
    assertions: AssertionListTableRow[];
    groupBy: AssertionGroupBy;
    filterOptions?: any;
    originalFilterOptions?: any;
    filteredCount?: number;
    searchMatchesCount?: number;
    totalCount?: number;
};

export type AssertionFilterOptions = {
    filterGroupOptions: {
        type: AssertionType[];
        status: AssertionResultType[];
        column: string[];
        tags: string[];
        source: AssertionSourceType[];
    };
    recommendedFilters: AssertionRecommendedFilter[];
};

export type AssertionRecommendedFilter = {
    name: string;
    category: 'status' | 'type' | 'source';
    count: number;
    displayName: string;
};

export type AssertionWithDescription = Assertion & { description: string };
