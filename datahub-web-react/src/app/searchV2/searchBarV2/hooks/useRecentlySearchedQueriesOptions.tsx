import React, { useMemo } from 'react';
<<<<<<< HEAD

import RecentSearch from '@app/searchV2/searchBarV2/components/RecentSearch';
import SectionHeader from '@app/searchV2/searchBarV2/components/SectionHeader';
import { RELEVANCE_QUERY_OPTION_TYPE } from '@app/searchV2/searchBarV2/constants';
import useRecentlySearchedQueries from '@app/searchV2/searchBarV2/hooks/useRecentlySearchedQueries';
=======
import useRecentlySearchedQueries from './useRecentlySearchedQueries';
import SectionHeader from '../components/SectionHeader';
import RecentSearch from '../components/RecentSearch';
import { RELEVANCE_QUERY_OPTION_TYPE } from '../constants';
>>>>>>> dbad52283b070c7cc136306c1553770db2f72105

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
