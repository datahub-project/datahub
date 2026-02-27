import {
    checkProductUpdateDismissed,
    checkShouldSkipWelcomeModal,
    dismissProductUpdate,
    setSkipWelcomeModal,
} from '@app/shared/localStorageUtils';

describe('localStorageUtils', () => {
    beforeEach(() => {
        localStorage.clear();
    });

    describe('checkShouldSkipWelcomeModal', () => {
        it('returns false when key is not set', () => {
            expect(checkShouldSkipWelcomeModal()).toBe(false);
        });

        it('returns true when key is set to "true"', () => {
            localStorage.setItem('skipWelcomeModal', 'true');
            expect(checkShouldSkipWelcomeModal()).toBe(true);
        });

        it('returns false when key is set to "false"', () => {
            localStorage.setItem('skipWelcomeModal', 'false');
            expect(checkShouldSkipWelcomeModal()).toBe(false);
        });

        it('returns false when key is set to other value', () => {
            localStorage.setItem('skipWelcomeModal', 'something');
            expect(checkShouldSkipWelcomeModal()).toBe(false);
        });
    });

    describe('setSkipWelcomeModal', () => {
        it('sets the key to "true" by default', () => {
            setSkipWelcomeModal();
            expect(localStorage.getItem('skipWelcomeModal')).toBe('true');
        });

        it('sets the key to "true" when passed true', () => {
            setSkipWelcomeModal(true);
            expect(localStorage.getItem('skipWelcomeModal')).toBe('true');
        });

        it('sets the key to "false" when passed false', () => {
            setSkipWelcomeModal(false);
            expect(localStorage.getItem('skipWelcomeModal')).toBe('false');
        });
    });

    describe('checkProductUpdateDismissed', () => {
        it('returns false when key is not set', () => {
            expect(checkProductUpdateDismissed('v1.0.0')).toBe(false);
        });

        it('returns false when JSON is invalid', () => {
            localStorage.setItem('dismissedProductUpdates', 'invalid json');
            expect(checkProductUpdateDismissed('v1.0.0')).toBe(false);
        });

        it('returns false when update ID is not in the list', () => {
            localStorage.setItem('dismissedProductUpdates', JSON.stringify(['v1.0.0', 'v2.0.0']));
            expect(checkProductUpdateDismissed('v3.0.0')).toBe(false);
        });

        it('returns true when update ID is in the list', () => {
            localStorage.setItem('dismissedProductUpdates', JSON.stringify(['v1.0.0', 'v2.0.0']));
            expect(checkProductUpdateDismissed('v1.0.0')).toBe(true);
        });
    });

    describe('dismissProductUpdate', () => {
        it('creates a new array with the update ID', () => {
            dismissProductUpdate('v1.0.0');
            const stored = JSON.parse(localStorage.getItem('dismissedProductUpdates') || '[]');
            expect(stored).toEqual(['v1.0.0']);
        });

        it('appends to existing array', () => {
            localStorage.setItem('dismissedProductUpdates', JSON.stringify(['v1.0.0']));
            dismissProductUpdate('v2.0.0');
            const stored = JSON.parse(localStorage.getItem('dismissedProductUpdates') || '[]');
            expect(stored).toEqual(['v1.0.0', 'v2.0.0']);
        });

        it('does not add duplicate IDs', () => {
            dismissProductUpdate('v1.0.0');
            dismissProductUpdate('v1.0.0');
            const stored = JSON.parse(localStorage.getItem('dismissedProductUpdates') || '[]');
            expect(stored).toEqual(['v1.0.0']);
        });

        it('handles invalid JSON by creating new array', () => {
            localStorage.setItem('dismissedProductUpdates', 'invalid json');
            dismissProductUpdate('v1.0.0');
            const stored = JSON.parse(localStorage.getItem('dismissedProductUpdates') || '[]');
            expect(stored).toEqual(['v1.0.0']);
        });
    });
});
