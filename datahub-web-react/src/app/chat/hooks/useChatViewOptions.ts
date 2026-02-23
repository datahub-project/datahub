import { useMemo } from 'react';

import { SelectOption } from '@components/components/Select/types';

import { buildViewSelectOptions } from '@app/chat/utils/chatViews';
import { useUserContext } from '@app/context/useUserContext';
import { DEFAULT_LIST_VIEWS_PAGE_SIZE } from '@app/entityV2/view/utils';

import { useListGlobalViewsQuery, useListMyViewsQuery } from '@graphql/view.generated';
import { DataHubView, DataHubViewType } from '@types';

interface UseChatViewOptionsReturn {
    viewOptions: SelectOption[];
    defaultViewValue: string;
    loading: boolean;
}

/**
 * Fetches the user's personal and global views, merges them into
 * SelectOption[] for use in the chat view selector.
 *
 * Also returns the default selected value based on the user's
 * globally-selected view (falls back to empty string for placeholder).
 *
 * @returns viewOptions, defaultViewValue, and loading state
 */
export function useChatViewOptions(): UseChatViewOptionsReturn {
    const userContext = useUserContext();
    const globalSelectedViewUrn = userContext.localState?.selectedViewUrn;

    const { data: personalData, loading: personalLoading } = useListMyViewsQuery({
        variables: { start: 0, count: DEFAULT_LIST_VIEWS_PAGE_SIZE, viewType: DataHubViewType.Personal },
        fetchPolicy: 'cache-first',
    });

    const { data: globalData, loading: globalLoading } = useListGlobalViewsQuery({
        variables: { start: 0, count: DEFAULT_LIST_VIEWS_PAGE_SIZE },
        fetchPolicy: 'cache-first',
    });

    const viewOptions = useMemo(() => {
        const personalViews = (personalData?.listMyViews?.views ?? []) as DataHubView[];
        const globalViews = (globalData?.listGlobalViews?.views ?? []) as DataHubView[];
        const allViews = [...personalViews, ...globalViews];
        return buildViewSelectOptions(allViews);
    }, [personalData, globalData]);

    const defaultViewValue = globalSelectedViewUrn ?? '';

    return {
        viewOptions,
        defaultViewValue,
        loading: personalLoading || globalLoading,
    };
}
