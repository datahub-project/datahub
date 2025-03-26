import { GenericEntityProperties } from '@src/app/entity/shared/types';
import { ActionRequestAssignee, FacetFilterInput } from '@src/types.generated';
import { cloneDeep } from 'lodash';

export const PERSONAL_ACTION_REQUESTS_GROUP_NAME = 'Inbox';
export const MY_PROPOSALS_GROUP_NAME = 'My Proposals';

export type ActionRequestGroup = {
    name: string;
    displayName: string;
    assignee?: ActionRequestAssignee;
    createdBy?: {
        urn: string;
    };
};

export const entityHasProposals = (entityData: GenericEntityProperties | null) => {
    if (!entityData) {
        return false;
    }

    return Array.isArray(entityData.proposals) && entityData.proposals.length > 0;
};

// Works only for filters with same filter operator and negated values
export const mergeFilters = (baseFilters: FacetFilterInput[], newFilters: FacetFilterInput[]): FacetFilterInput[] => {
    if (!newFilters || !newFilters.length) {
        return baseFilters;
    }
    const filters: FacetFilterInput[] = cloneDeep(baseFilters);
    const keys = new Set(filters.map((f) => f.field));

    newFilters.forEach((filter) => {
        if (keys.has(filter.field)) {
            const exisitingFilter = filters.find((f) => f.field === filter.field) as FacetFilterInput;
            exisitingFilter.values = Array.from(
                new Set([...(exisitingFilter?.values || []), ...(filter?.values || [])]),
            );
        } else {
            filters.push(filter);
        }
    });

    return filters;
};
