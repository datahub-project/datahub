import { getDefaultLandingPage } from '@app/defaultLandingPage';
import { PageRoutes } from '@conf/Global';

describe('getDefaultLandingPage', () => {
    it('returns Discover when configured', () => {
        expect(getDefaultLandingPage('discover')).toBe(PageRoutes.SEARCH);
    });

    it('returns Home by default', () => {
        expect(getDefaultLandingPage()).toBe(PageRoutes.HOME);
    });

    it('ignores surrounding whitespace and casing', () => {
        expect(getDefaultLandingPage('  DISCOVER  ')).toBe(PageRoutes.SEARCH);
    });

    it('falls back to Home for unsupported values', () => {
        expect(getDefaultLandingPage('unknown')).toBe(PageRoutes.HOME);
    });
});