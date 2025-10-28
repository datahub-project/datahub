import { DataHubSubscription, EntityType } from '@types';

export type SubscriptionListFilter = {
    sortBy: string;
    groupBy: string;
    filterCriteria: {
        searchText: string;
        entity: EntityType[];
        owner: string[];
        eventType: string[];
    };
};

export type SubscriptionRecommendedFilter = {
    name: string;
    category: 'entity' | 'destination' | 'owner' | 'eventType';
    count: number;
    displayName: string;
};

export type SubscriptionFilterOptions = {
    filterGroupOptions: {
        entity: SubscriptionRecommendedFilter[];
        owner: SubscriptionRecommendedFilter[];
        eventType: SubscriptionRecommendedFilter[];
    };
    recommendedFilters: SubscriptionRecommendedFilter[];
};

export type SubscriptionListTableRow = {
    key: string;
    subscription: DataHubSubscription;
    entity?: EntityType;
    destination?: string;
    owner?: string;
    eventType?: string;
    createdOn?: number;
    updatedOn?: number;
};
