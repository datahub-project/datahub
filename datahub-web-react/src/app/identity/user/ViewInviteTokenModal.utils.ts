import { CorpUser } from '@types';

/**
 * Formats numbers for display in the UI (e.g., 1500 -> "1.5k")
 */
export const formatNumber = (num: number): string => {
    if (num >= 1000) {
        return `${(num / 1000).toFixed(1)}k`;
    }
    return num.toString();
};

/**
 * Filters and sorts users by usage patterns to generate recommendations
 */
export const generateUserRecommendations = (users: CorpUser[], maxRecommendations = 10): CorpUser[] => {
    // Filter users who haven't been invited yet and have usage data
    const eligibleUsers = users.filter((user) => {
        const hasUsageFeatures =
            user.usageFeatures?.userUsageTotalPast30Days && user.usageFeatures.userUsageTotalPast30Days > 0;
        const hasNoInvitation = !user.invitationStatus;
        const hasEmail = user.info?.email || user.editableProperties?.email;

        // Show users with email addresses and usage data
        return hasNoInvitation && hasEmail && hasUsageFeatures;
    });

    // Sort by query count (most active first)
    const recommendations = eligibleUsers.sort((a, b) => {
        const aQueries = a.usageFeatures?.userUsageTotalPast30Days || 0;
        const bQueries = b.usageFeatures?.userUsageTotalPast30Days || 0;
        return bQueries - aQueries;
    });

    return recommendations.slice(0, maxRecommendations);
};

/**
 * Creates a personalized invite link for a specific user
 */
export const createUserInviteLink = (baseInviteLink: string, username: string): string => {
    return `${baseInviteLink}&user_hint=${encodeURIComponent(username)}`;
};
