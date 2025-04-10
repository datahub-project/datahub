import { Dispatch, SetStateAction } from 'react';
import {
    AuditStamp,
    CorpUser,
    DataPlatform,
    EntityPrivileges,
    EntityType,
    Incident,
    IncidentPriority,
    IncidentSource,
    IncidentStage,
    IncidentState,
    IncidentType,
    OwnerType,
} from '@src/types.generated';
import { BaseItemType } from '@src/alchemy-components/components/Timeline/types';
import { IncidentAction } from './constant';

export type IncidentListFilter = {
    sortBy: string;
    groupBy: string;
    filterCriteria: {
        searchText: string;
        priority: string[];
        stage: string[];
        category: string[];
        state: string[];
    };
};

export type IncidentGroupBy = {
    priority: IncidentGroup[];
    stage: IncidentGroup[];
    category: IncidentGroup[];
    state: IncidentGroup[];
};

export type IncidentTable = {
    incidents: IncidentTableRow[];
    groupBy: IncidentGroupBy;
    filterOptions?: any;
    originalFilterOptions?: any;
    searchMatchesCount?: number;
    totalCount?: number;
};

export type IncidentGroup = {
    name: string;
    icon: React.ReactNode;
    description?: string;
    incidents: IncidentTableRow[];
    untransformedIncidents: Incident[];
    // summary?: IncidentStatusSummary;
    type?: IncidentType;
    stage?: IncidentStage;
    state?: IncidentState;
    priority?: IncidentPriority;
    groupName?: JSX.Element;
};

export type IncidentFilterOptions = {
    filterGroupOptions: {
        category: IncidentType[];
        stage: IncidentStage[];
        priority: IncidentPriority[];
        state: IncidentState[];
    };
    recommendedFilters: IncidentRecommendedFilter[];
};

export type IncidentRecommendedFilter = {
    name: string;
    category: 'type' | 'stage' | 'priority' | 'state';
    count: number;
    displayName: string;
};

export type IncidentTableRow = {
    urn: string;
    created: number;
    creator: AuditStamp;
    customType: string;
    description: string;
    stage: IncidentStage;
    state: IncidentState;
    type: IncidentType;
    title: string;
    priority: IncidentPriority;
    source: IncidentSource;
    assignees: Array<OwnerType>;
    linkedAssets: any[];
    message: string;
    lastUpdated: AuditStamp;
};

export type IncidentEditorProps = {
    incidentUrn?: string;
    refetch?: () => void;
    onSubmit?: (incident?: Incident) => void;
    onClose?: () => void;
    data?: IncidentTableRow;
    mode?: IncidentAction;
    entity?: EntityStagedForIncident;
    urn?: string;
};

export type IncidentLinkedAssetsListProps = {
    initialUrn?: string;
    form: any;
    data?: IncidentTableRow;
    mode: IncidentAction;
    setCachedLinkedAssets: React.Dispatch<React.SetStateAction<any[]>>;
    setIsLinkedAssetsLoading: React.Dispatch<React.SetStateAction<boolean>>;
    urn?: string;
};

export interface TimelineContentDetails extends BaseItemType {
    action: string;
    actor: CorpUser;
    time: number;
    message?: string;
}

export enum IncidentConstant {
    PRIORITY = 'priority',
    STAGE = 'stage',
    CATEGORY = 'category',
    STATE = 'state',
}

export type EntityStagedForIncident = {
    urn: string;
    platform?: DataPlatform;
    entityType?: EntityType;
};

export type IncidentBuilderSiblingOptions = {
    title: string;
    disabled?: boolean;
} & Partial<EntityStagedForIncident>;

export type IncidentHandlerProps = {
    mode: IncidentAction;
    onSubmit?: () => void;
    incidentUrn: string | undefined;
    user: CorpUser | null | undefined;
    entity: EntityStagedForIncident | undefined;
    assignees: CorpUser[];
    linkedAssets: string[];
};
