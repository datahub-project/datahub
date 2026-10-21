import { getDefaultLandingPage, getHomePageRoute } from '@app/defaultLandingPage';
import { PageRoutes } from '@conf/Global';

describe('getDefaultLandingPage', () => {
    it('returns Discover when configured', () => {
        expect(getDefaultLandingPage('discover')).toBe(PageRoutes.SEARCH);
    });

    it('keeps the existing root route for Home and when unset', () => {
        expect(getDefaultLandingPage('home')).toBeUndefined();
        expect(getDefaultLandingPage()).toBeUndefined();
    });

    it('ignores surrounding whitespace and casing', () => {
        expect(getDefaultLandingPage('  DISCOVER  ')).toBe(PageRoutes.SEARCH);
    });

    it('keeps the existing root route for unsupported values', () => {
        expect(getDefaultLandingPage('unknown')).toBeUndefined();
    });
});

describe('getHomePageRoute', () => {
    it('preserves the root Home route unless Discover is configured', () => {
        expect(getHomePageRoute()).toBe(PageRoutes.ROOT);
        expect(getHomePageRoute('home')).toBe(PageRoutes.ROOT);
        expect(getHomePageRoute('discover')).toBe(PageRoutes.HOME);
    });
});
