const STORAGE_KEY = 'datahub_seen_recommended_users';

/**
 * Check if the user has previously visited the Manage Users & Groups page.
 * Returns true if they have seen it before, false otherwise.
 */
export function hasSeenRecommendedUsers(): boolean {
    try {
        return localStorage.getItem(STORAGE_KEY) === 'true';
    } catch (e) {
        // localStorage might be disabled or unavailable
        return false;
    }
}

/**
 * Mark that the user has visited the Manage Users & Groups page.
 * This flag persists across sessions and is never cleared.
 */
export function markRecommendedUsersAsSeen(): void {
    try {
        localStorage.setItem(STORAGE_KEY, 'true');
    } catch (e) {
        // localStorage might be disabled or unavailable
        console.warn('Failed to save recommended users seen state:', e);
    }
}
