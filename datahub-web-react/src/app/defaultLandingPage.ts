import { PageRoutes } from '@conf/Global';

export function getDefaultLandingPage(value?: string): string {
    switch (value?.trim().toLowerCase()) {
        case 'discover':
            return PageRoutes.SEARCH;
        case 'home':
        default:
            return PageRoutes.HOME;
    }
}