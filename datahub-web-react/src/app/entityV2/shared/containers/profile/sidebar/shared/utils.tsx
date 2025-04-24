import moment from 'moment';

import { REDESIGN_COLORS } from '@app/entityV2/shared/constants';
import { GenericEntityProperties } from '@src/app/entity/shared/types';
import { EntityRegistry } from '@src/entityRegistryContext';

import { CorpUser, ShareResult, ShareResultState } from '@types';

/**
 * A tier of popularity for the dataset.
 */
export enum PopularityTier {
    TIER_1, // Most Popular
    TIER_2,
    TIER_3,
    TIER_4, // Least Popular
}

export const ACRYL_PLATFORM = 'DataHub Cloud';

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

/**
 * Returns the status for a given share or unshare result to display the appropraite
 * cues to the user.
 */
export function getShareResultStatus(result?: ShareResult) {
    const isRunning = result?.status === ShareResultState.Running;
    const isAttemptedRecently = moment() < moment(result?.statusLastUpdated).add(5, 'minutes');
    const isInProgress = isRunning && isAttemptedRecently;
    const failed = result?.status === ShareResultState.Failure || (isRunning && !isAttemptedRecently);
    const partialSuccess = result?.status === ShareResultState.PartialSuccess;
    return { isInProgress, failed, partialSuccess };
}

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

interface SharedItemStatusProps {
    shareResult: ShareResult;
    unshareResult?: ShareResult;
}

function getSharedItemStatus({ shareResult, unshareResult }: SharedItemStatusProps) {
    const isShareMoreRecent = (shareResult?.statusLastUpdated || 1) > (unshareResult?.statusLastUpdated || 0);
    const {
        isInProgress: isSharing,
        failed: failedToShare,
        partialSuccess,
    } = isShareMoreRecent
        ? getShareResultStatus(shareResult)
        : { isInProgress: false, failed: false, partialSuccess: false };
    const { isInProgress: isUnsharing, failed: failedToUnshare } = isShareMoreRecent
        ? { isInProgress: false, failed: false }
        : getShareResultStatus(unshareResult);

    const isInProgress = isSharing || isUnsharing;
    const hasFailed = failedToShare || failedToUnshare;

    return { isInProgress, isUnsharing, hasFailed, failedToUnshare, partialSuccess };
}

interface SharedItemInfoProps {
    shareResult: ShareResult;
    entityData?: GenericEntityProperties | null;
    entityRegistry: EntityRegistry;
}

export function getSharedItemInfo({ shareResult, entityData, entityRegistry }: SharedItemInfoProps) {
    const unshareResult = entityData?.share?.lastUnshareResults?.find(
        (r) =>
            r.destination?.urn === shareResult.destination?.urn &&
            r.implicitShareEntity?.urn === shareResult.implicitShareEntity?.urn,
    );
    const lastSuccessTime = shareResult.lastSuccess?.time || 0;
    const lastAttempt = shareResult.lastAttempt.time || 0;
    const hasSharedLineage =
        shareResult.shareConfig?.enableDownstreamLineage || shareResult.shareConfig?.enableUpstreamLineage;
    const name = shareResult.destination?.details.name || shareResult.destination?.urn || 'Deleted connection';
    const implicitShareEntity = (shareResult as any)?.implicitShareEntity;
    const linkedEntityName = implicitShareEntity
        ? entityRegistry.getDisplayName(implicitShareEntity?.type, implicitShareEntity)
        : '';
    const linkedEntityUrl = implicitShareEntity
        ? entityRegistry.getEntityUrl(implicitShareEntity.type, implicitShareEntity.urn)
        : null;

    const { isInProgress, isUnsharing, hasFailed, failedToUnshare, partialSuccess } = getSharedItemStatus({
        shareResult,
        unshareResult,
    });

    return {
        linkedEntityUrl,
        linkedEntityName,
        hasSharedLineage,
        isInProgress,
        isUnsharing,
        hasFailed,
        failedToUnshare,
        partialSuccess,
        lastSuccessTime,
        lastAttempt,
        name,
    };
}
