import { SubscriptionListFilter } from '@app/settingsV2/personal/subscriptions/types';

export const SUBSCRIPTION_DEFAULT_FILTERS: SubscriptionListFilter = {
    sortBy: '',
    groupBy: 'entity',
    filterCriteria: {
        searchText: '',
        entity: [],
        owner: [],
        eventType: [],
    },
};
