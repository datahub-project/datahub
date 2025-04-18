import { useAppConfig } from '../useAppConfig';

const useSearchAndBrowseVersion = () => {
    const appConfig = useAppConfig();
    const searchVersion = appConfig.config.featureFlags.showSearchFiltersV2 ? 'v2' : 'v1';
    const browseVersion = appConfig.config.featureFlags.showBrowseV2 ? 'v2' : 'v1';
    const isPlatformBrowse = appConfig.config.featureFlags.platformBrowseV2;
    return { searchVersion, browseVersion, isPlatformBrowse } as const;
};

export const useSearchVersion = () => {
    return useSearchAndBrowseVersion().searchVersion;
};

export const useBrowseVersion = () => {
    return useSearchAndBrowseVersion().browseVersion;
};

export const useIsSearchV2 = () => {
    return useSearchVersion() === 'v2';
};

// only show browseV2 if search filtersV2 is also enabled
export const useIsBrowseV2 = () => {
    const browseVersion = useBrowseVersion();
    const searchFiltersVersion = useSearchVersion();

    return browseVersion === 'v2' && searchFiltersVersion === 'v2';
};

export const useIsPlatformBrowseV2 = () => {
    const { isPlatformBrowse } = useSearchAndBrowseVersion();
    return isPlatformBrowse;
};
