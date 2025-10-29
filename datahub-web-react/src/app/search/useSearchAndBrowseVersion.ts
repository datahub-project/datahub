import { useAppConfig } from '@app/useAppConfig';

const useSearchAndBrowseVersion = () => {
    const appConfig = useAppConfig();
    const searchVersion = appConfig.config.featureFlags.showSearchFiltersV2 ? 'v2' : 'v1';
    const browseVersion = appConfig.config.featureFlags.showBrowseV2 ? 'v2' : 'v1';

    return { searchVersion, browseVersion } as const;
};

export const useSearchVersion = () => {
    return useSearchAndBrowseVersion().searchVersion;
};

const useBrowseVersion = () => {
    return useSearchAndBrowseVersion().browseVersion;
};
