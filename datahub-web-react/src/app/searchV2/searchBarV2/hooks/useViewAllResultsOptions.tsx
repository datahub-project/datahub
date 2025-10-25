import React, { useMemo } from 'react';

import AskDataHub from '@app/searchV2/searchBarV2/components/AskDataHub';
import ViewAllResults from '@app/searchV2/searchBarV2/components/ViewAllResults';
import { ASK_DATAHUB_OPTION_TYPE, EXACT_AUTOCOMPLETE_OPTION_TYPE } from '@app/searchV2/searchBarV2/constants';
import { EXACT_SEARCH_PREFIX } from '@app/searchV2/utils/constants';
import { useIsAiChatEnabled } from '@app/useAppConfig';

export default function useViewAllResultsOptions(query: string, shouldShow?: boolean) {
    const isChatEnabled = useIsAiChatEnabled();

    return useMemo(() => {
        if (query === '' || !shouldShow) return [];

        const options: { value: string; label: React.ReactNode; type: string }[] = [];

        // Only show "View all results" if chat is NOT enabled
        if (!isChatEnabled) {
            options.push({
                value: `${EXACT_SEARCH_PREFIX}${query}`,
                label: <ViewAllResults searchText={query} dataTestId="view-all-results" />,
                type: EXACT_AUTOCOMPLETE_OPTION_TYPE,
            });
        }

        // Add "Ask DataHub" option if chat is enabled
        if (isChatEnabled) {
            options.push({
                value: `ask_datahub:${query}`,
                label: <AskDataHub searchText={query} />,
                type: ASK_DATAHUB_OPTION_TYPE,
            });
        }

        return options;
    }, [query, shouldShow, isChatEnabled]);
}
