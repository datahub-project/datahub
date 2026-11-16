import { SKIP_WELCOME_MODAL_KEY } from '@app/sharedV2/constants';

/**
 * Check if the welcome modal has been dismissed/skipped by the user
 */
export function checkShouldSkipWelcomeModal(): boolean {
    return localStorage.getItem(SKIP_WELCOME_MODAL_KEY) === 'true';
}

/**
 * Mark the welcome modal as dismissed/skipped
 */
export function setSkipWelcomeModal(skip = true): void {
    localStorage.setItem(SKIP_WELCOME_MODAL_KEY, String(skip));
}
