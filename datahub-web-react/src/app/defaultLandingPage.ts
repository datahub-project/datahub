import { PageRoutes } from '@conf/Global';

export function getDefaultLandingPage(value?: string): string | undefined {
    switch (value?.trim().toLowerCase()) {
        case 'discover':
            return PageRoutes.SEARCH;
        case 'home':
        default:
            return undefined;
    }
}

export function getHomePageRoute(value?: string): string {
    return getDefaultLandingPage(value) ? PageRoutes.HOME : PageRoutes.ROOT;
}
