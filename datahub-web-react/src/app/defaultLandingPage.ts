import { PageRoutes } from '@conf/Global';

export function getDefaultLandingPage(value?: string): string | undefined {
    return value?.trim().toLowerCase() === 'discover' ? PageRoutes.SEARCH : undefined;
}
