import { DATASET_FEATURES_SORT_OPTIONS, SORT_OPTIONS } from '@app/searchV2/context/constants';
import { useIsDatasetFeaturesSearchSortEnabled } from '@src/app/useAppConfig';

export default function useGetSortOptions() {
    const isDatasetFeaturesSearchSortEnabled = useIsDatasetFeaturesSearchSortEnabled();
    if (isDatasetFeaturesSearchSortEnabled) {
        return { ...SORT_OPTIONS, ...DATASET_FEATURES_SORT_OPTIONS };
    }

    // TODO: Add a new endpoint showSortFields() that passes the list of potential sort fields, and verifies
    // whether there are any entries matching that sort field.
    return SORT_OPTIONS;
}
