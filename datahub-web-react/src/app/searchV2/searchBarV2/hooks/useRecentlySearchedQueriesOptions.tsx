import React, { useMemo } from 'react';

import RecentSearch from '@app/searchV2/searchBarV2/components/RecentSearch';
import SectionHeader from '@app/searchV2/searchBarV2/components/SectionHeader';
import { RELEVANCE_QUERY_OPTION_TYPE } from '@app/searchV2/searchBarV2/constants';
import useRecentlySearchedQueries from '@app/searchV2/searchBarV2/hooks/useRecentlySearchedQueries';
import { SectionOption } from '@app/searchV2/searchBarV2/types';

export default function useRecentlySearchedQueriesOptions(): SectionOption[] {
    const { recentlySearchedQueries } = useRecentlySearchedQueries();
    const recentlySearchedQueriesOptions = useMemo(() => {
        if (recentlySearchedQueries === undefined || recentlySearchedQueries.length === 0) return [];

        return [
            {
                label: <SectionHeader text="You Recently Searched" />,
                options: recentlySearchedQueries.map((content) => ({
                    value: content.value,
                    label: <RecentSearch text={content.value} />,
                    type: RELEVANCE_QUERY_OPTION_TYPE,
                })),
            },
        ];
    }, [recentlySearchedQueries]);

    return recentlySearchedQueriesOptions;
}
