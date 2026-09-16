import useAggregationsQuery from '@app/searchV2/sidebar/useAggregationsQuery';
import { PLATFORM_FILTER_NAME } from '@app/searchV2/utils/constants';

type Props = {
    skip: boolean;
};

const useSidebarPlatforms = ({ skip }: Props) => {
    const {
        error: filteredError,
        platformAggregations: filteredAggs,
        retry: retryFilteredAggs,
    } = useAggregationsQuery({
        skip,
        facets: [PLATFORM_FILTER_NAME],
    });

    // Show every platform that has results for the current query. We intentionally
    // do NOT intersect with an unfiltered ("base") aggregation: the base aggregation
    // is a top-N terms query, so any platform ranked below that cap (e.g. a long-tail
    // source like Confluence on a dataset-heavy instance) would be dropped from the
    // sidebar even when it matches the active search.
    return { error: filteredError, platformAggregations: filteredAggs, retry: retryFilteredAggs } as const;
};

export default useSidebarPlatforms;
