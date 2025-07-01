import { debounce } from 'lodash';
import { useEffect, useState } from 'react';

import { useUserContext } from '@app/context/useUserContext';
import { HALF_SECOND_IN_MS, MAX_ROWS_BEFORE_DEBOUNCE } from '@app/entityV2/shared/tabs/Dataset/Queries/utils/constants';

import { useListGlobalViewsQuery, useListMyViewsQuery } from '@graphql/view.generated';
import { DataHubViewType } from '@types';

import { DEFAULT_LIST_VIEWS_PAGE_SIZE } from '../../utils';
import { filterViews } from '../utils';

export default function useViewsData() {
    const userContext = useUserContext();

    const [selectedUrn, setSelectedUrn] = useState<string | undefined>(
        userContext.localState?.selectedViewUrn || undefined,
    );

    const [hoverViewUrn, setHoverViewUrn] = useState<string | undefined>(undefined);

    const [selectedViewName, setSelectedView] = useState<string>('');

    const [filterText, setFilterText] = useState<string>('');

    /**
     * Queries - Notice, each of these queries is cached. Here we fetch both the user's private views,
     * along with all public views.
     */
    const { data: privateViewsData } = useListMyViewsQuery({
        variables: {
            start: 0,
            count: DEFAULT_LIST_VIEWS_PAGE_SIZE,
            viewType: DataHubViewType.Personal,
        },
        fetchPolicy: 'cache-first',
    });

    // Fetch Public Views
    const { data: publicViewsData } = useListGlobalViewsQuery({
        variables: {
            start: 0,
            count: DEFAULT_LIST_VIEWS_PAGE_SIZE,
        },
        fetchPolicy: 'cache-first',
    });

    useEffect(() => {
        setSelectedUrn(userContext.localState?.selectedViewUrn || undefined);
        const selectedView =
            privateViewsData?.listMyViews?.views?.find(
                (view) => view?.urn === userContext.localState?.selectedViewUrn,
            ) ||
            publicViewsData?.listGlobalViews?.views?.find(
                (view) => view?.urn === userContext.localState?.selectedViewUrn,
            );
        if (selectedView === undefined) {
            setSelectedView('');
        } else {
            setSelectedView(selectedView.name);
        }
    }, [userContext.localState?.selectedViewUrn, setSelectedUrn, privateViewsData, publicViewsData]);

    const highlightedPublicViewData = filterViews(filterText, publicViewsData?.listGlobalViews?.views || []);
    const highlightedPrivateViewData = filterViews(filterText, privateViewsData?.listMyViews?.views || []);

    const debouncedSetFilterText = debounce(
        (e: React.ChangeEvent<HTMLInputElement>) => setFilterText(e.target.value),
        (highlightedPublicViewData.length || highlightedPrivateViewData.length) > MAX_ROWS_BEFORE_DEBOUNCE
            ? HALF_SECOND_IN_MS
            : 0,
    );

    const privateViews = highlightedPrivateViewData || [];
    const publicViews = highlightedPublicViewData || [];
    const privateViewCount = privateViews?.length || 0;
    const publicViewCount = publicViews?.length || 0;
    const hasViews = privateViewCount > 0 || publicViewCount > 0 || false;

    return {
        selectedUrn,
        setSelectedUrn,
        hoverViewUrn,
        setHoverViewUrn,
        selectedViewName,
        setSelectedView,
        highlightedPublicViewData,
        highlightedPrivateViewData,
        privateViews,
        privateViewCount,
        publicViews,
        publicViewCount,
        hasViews,
        debouncedSetFilterText,
        filterText,
        setFilterText,
    };
}
