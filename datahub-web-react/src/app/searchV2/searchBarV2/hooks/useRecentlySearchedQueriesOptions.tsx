import React, { useMemo } from 'react';
import useRecentlySearchedQueries from './useRecentlySearchedQueries';
import SectionHeader from '../components/SectionHeader';
import RecentSearch from '../components/RecentSearch';
import { RELEVANCE_QUERY_OPTION_TYPE } from '../constants';

export default function useRecentlySearchedQueriesOptions() {
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
