import { checkShouldSkipWelcomeModal, setSkipWelcomeModal } from '@app/sharedV2/localStorageUtils';

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
});
