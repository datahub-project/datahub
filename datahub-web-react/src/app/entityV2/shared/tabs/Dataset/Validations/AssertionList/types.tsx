import {
    AssertionResultType,
    AssertionRunEvent,
    AssertionRunStatus,
    AuditStamp,
    DataPlatform,
    GlobalTags,
    Monitor,
} from '@src/types.generated';
import { AssertionWithMonitorDetails } from '../acrylUtils';
import { AssertionGroup } from '../acrylTypes';

export type AssertionListFilter = {
    sortBy: string;
    groupBy: string;
    filterCriteria: {
        searchText: string;
        status: string[];
        type: string[];
        tags: string[];
        columns: string[];
    };
};

export type AssertionListTableRow = {
    type?: string;
    lastUpdated?: AuditStamp;
    tags: GlobalTags;
    descriptionHTML: JSX.Element | null;
    description: string;
    urn: string;
    platform: DataPlatform;
    lastEvaluation?: AssertionRunEvent;
    lastEvaluationTimeMs?: number;
    lastEvaluationResult?: AssertionResultType; // add type
    lastEvaluationUrl?: string;
    assertion: AssertionWithMonitorDetails;
    monitor: Monitor;
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

type AssertionGroupBy = {
    type: any[];
    status: AssertionStatusGroup[];
};

export type AssertionTable = {
    assertions: AssertionListTableRow[];
    groupBy: AssertionGroupBy;
};
