// Global state for tracking invited users across invitation methods
// This prevents circular imports between hooks and components

const globalInvitedUsers = new Set<string>(); // Track all invited users across all invitation methods
let hasInvitedYet = false; // Track if user has invited anyone to control refetch behavior

// Helper function to add users to global tracking
export const addToGlobalInvitedUsers = (userIdentifiers: string[]) => {
    userIdentifiers.forEach((identifier) => {
        if (identifier) globalInvitedUsers.add(identifier);
    });
    hasInvitedYet = true; // Mark that user has invited someone
};

// Get all globally invited users
export const getGlobalInvitedUsers = (): Set<string> => {
    return new Set(globalInvitedUsers); // Return a copy to prevent external mutations
};

// Check if user has invited anyone yet
export const getHasInvitedYet = (): boolean => {
    return hasInvitedYet;
};

// Reset the invitation flag (for modal close/reset)
export const resetHasInvitedYet = (): void => {
    hasInvitedYet = false;
};
