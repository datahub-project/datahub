import { FacetFilterInput, FilterOperator } from '@types';

export interface BuildRecommendedUsersFiltersOptions {
    selectedPlatforms?: string[];
}

/**
 * Builds filter configuration for recommended users queries.
 * Recommended users are:
 * - Not invited users (no invitationStatus)
 * - Have usage data (userUsageTotalPast30DaysFeature > 0)
 * - Are inactive (active field doesn't exist OR active=false)
 * - Optionally filtered by platform usage
 */
export function buildRecommendedUsersFilters(
    options?: BuildRecommendedUsersFiltersOptions,
): Array<{ and: FacetFilterInput[] }> {
    const { selectedPlatforms = [] } = options || {};

    const commonFilters: FacetFilterInput[] = [
        {
            field: 'invitationStatus',
            values: [''],
            condition: FilterOperator.Exists,
            negated: true,
        },
        {
            field: 'userUsageTotalPast30DaysFeature',
            values: ['0'],
            condition: FilterOperator.GreaterThan,
        },
    ];

    const inactiveUsersOrFilters = [
        {
            and: [
                ...commonFilters,
                {
                    field: 'active',
                    values: [''],
                    condition: FilterOperator.Exists,
                    negated: true,
                },
            ],
        },
        {
            and: [
                ...commonFilters,
                {
                    field: 'active',
                    values: ['false'],
                    condition: FilterOperator.Equal,
                },
            ],
        },
    ];

    if (selectedPlatforms.length === 0) {
        return inactiveUsersOrFilters;
    }

    const platformFilters: FacetFilterInput[] = selectedPlatforms.map((platform) => {
        const platformField = `platformUsageTotal.${platform}`;
        return {
            field: platformField,
            values: ['0'],
            condition: FilterOperator.GreaterThan,
        };
    });

    // Combine inactive users filters with platform filters
    // Cross-product: (inactive_branch_1 OR inactive_branch_2) AND (platform_1 OR platform_2 OR ...)
    const result: Array<{ and: FacetFilterInput[] }> = [];

    inactiveUsersOrFilters.forEach((inactiveFilter) => {
        platformFilters.forEach((platformFilter) => {
            result.push({
                and: [...(inactiveFilter.and || []), platformFilter],
            });
        });
    });

    return result;
}
