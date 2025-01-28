import moment from 'moment';
import { REDESIGN_COLORS } from '../../../../constants';
import { CorpUser } from '../../../../../../../types.generated';

/**
 * A tier of popularity for the dataset.
 */
export enum PopularityTier {
    TIER_1, // Most Popular
    TIER_2,
    TIER_3,
    TIER_4, // Least Popular
}

export const ACRYL_PLATFORM = 'Acryl';

export enum ActionType {
    SHARE,
    SYNC,
}

/**
 * Retrieve a tier of popularity for dataset given statistics about the dataset.
 * The tier is a function of the query count + unique users
 *
 * Tier 1: The dataset is in the top 20% of queries or unique users.
 * Tier 2: The dataset is in the top 70% of queries or unique users.
 * Tier 3: The dataset is in the bottom 30% of queries or unique users.
 * Tier 4: No usage: The dataset has no usage or unique users
 */
export const getDatasetPopularityTier = (queryCountPercentileLast30Days, uniqueUserPercentileLast30Days) => {
    if (queryCountPercentileLast30Days > 80 || uniqueUserPercentileLast30Days > 80) {
        return PopularityTier.TIER_1;
    }
    if (queryCountPercentileLast30Days > 30 || uniqueUserPercentileLast30Days > 30) {
        return PopularityTier.TIER_2;
    }
    if (queryCountPercentileLast30Days > 0 || uniqueUserPercentileLast30Days > 0) {
        return PopularityTier.TIER_3;
    }
    return PopularityTier.TIER_4;
};

/**
 * Retrieve a tier of popularity for dashboard given statistics about the dashboard.
 * The tier is a function of the view count + unique users
 *
 * Tier 1: The dashboard is in the top 20% of queries or unique users.
 * Tier 2: The dashboard is in the top 70% of queries or unique users.
 * Tier 3: The dashboard is in the bottom 30% of queries or unique users.
 * Tier 4: No usage: The dashboard has no usage or unique users
 */
export const getDashboardPopularityTier = (viewCountPercentileLast30Days, uniqueUserPercentileLast30Days) => {
    if (viewCountPercentileLast30Days > 80 || uniqueUserPercentileLast30Days > 80) {
        return PopularityTier.TIER_1;
    }
    if (viewCountPercentileLast30Days > 30 || uniqueUserPercentileLast30Days > 30) {
        return PopularityTier.TIER_2;
    }
    if (viewCountPercentileLast30Days > 0 || uniqueUserPercentileLast30Days > 0) {
        return PopularityTier.TIER_3;
    }
    return PopularityTier.TIER_4;
};

/**
 * Retrieve a tier of popularity for chart given statistics about the chart.
 * The tier is a function of the view count + unique users
 *
 * Tier 1: The chart is in the top 20% of queries or unique users.
 * Tier 2: The chart is in the top 70% of queries or unique users.
 * Tier 3: The chart is in the bottom 30% of queries or unique users.
 * Tier 4: No usage: The chart has no usage or unique users
 */
export const getChartPopularityTier = (viewCountPercentileLast30Days, uniqueUserPercentileLast30Days) => {
    if (viewCountPercentileLast30Days > 80 || uniqueUserPercentileLast30Days > 80) {
        return PopularityTier.TIER_1;
    }
    if (viewCountPercentileLast30Days > 30 || uniqueUserPercentileLast30Days > 30) {
        return PopularityTier.TIER_2;
    }
    if (viewCountPercentileLast30Days > 0 || uniqueUserPercentileLast30Days > 0) {
        return PopularityTier.TIER_3;
    }
    return PopularityTier.TIER_4;
};

export const getQueryPopularityTier = (runsPercentileLast30days: number) => {
    if (runsPercentileLast30days > 80) {
        return PopularityTier.TIER_1;
    }
    if (runsPercentileLast30days > 30) {
        return PopularityTier.TIER_2;
    }
    if (runsPercentileLast30days > 0) {
        return PopularityTier.TIER_3;
    }
    return PopularityTier.TIER_4;
};

/**
 * Returns true if the user "exists", e.g. there exists some information about
 * the user in DataHub already.
 *
 * @param user the user under consideration
 */
export const userExists = (user: CorpUser) => {
    return user.editableProperties || user.properties || user.isNativeUser;
};

export function isValuePresent(value) {
    return value !== null && value !== undefined && !Number.isNaN(value);
}

export const getBarsStatusFromPopularityTier = (tier: number) => {
    let status = 0;
    if (tier === 0) status = 3;
    else if (tier === 1) status = 2;
    else if (tier === 2) status = 1;
    return status;
};

export function getRelativeTimeColor(time: number) {
    const relativeTime = moment(time);
    if (relativeTime.isAfter(moment().subtract(1, 'week'))) {
        return `${REDESIGN_COLORS.GREEN_NORMAL}`;
    }
    if (relativeTime.isAfter(moment().subtract(1, 'month'))) {
        return `${REDESIGN_COLORS.WARNING_YELLOW}`;
    }
    return `${REDESIGN_COLORS.WARNING_RED}`;
}
