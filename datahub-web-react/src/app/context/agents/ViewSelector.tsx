import { SimpleSelect } from '@components';
import React, { useMemo, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { useDebounce } from 'react-use';

import { DEFAULT_LIST_VIEWS_PAGE_SIZE } from '@app/entityV2/view/utils';

import { useGetViewQuery, useListGlobalViewsQuery, useListMyViewsQuery } from '@graphql/view.generated';
import { DataHubViewType } from '@types';

const SEARCH_DEBOUNCE_MS = 300;

interface ViewSelectorProps {
    value: string | undefined;
    onChange: (viewUrn: string | undefined) => void;
}

/** Picks one of the viewer's personal Views or a global View. */
export const ViewSelector: React.FC<ViewSelectorProps> = ({ value, onChange }) => {
    const { t } = useTranslation('settings.ai');
    const [searchText, setSearchText] = useState('');
    const [query, setQuery] = useState('');
    useDebounce(() => setQuery(searchText.trim()), SEARCH_DEBOUNCE_MS, [searchText]);

    // Search on the server so Views past the first page can still be found.
    const { data: personalData, loading: personalLoading } = useListMyViewsQuery({
        variables: {
            start: 0,
            count: DEFAULT_LIST_VIEWS_PAGE_SIZE,
            viewType: DataHubViewType.Personal,
            query: query || undefined,
        },
        fetchPolicy: 'cache-first',
    });
    const { data: globalData, loading: globalLoading } = useListGlobalViewsQuery({
        variables: { start: 0, count: DEFAULT_LIST_VIEWS_PAGE_SIZE, query: query || undefined },
        fetchPolicy: 'cache-first',
    });

    const listedOptions = useMemo(
        () =>
            [...(personalData?.listMyViews?.views ?? []), ...(globalData?.listGlobalViews?.views ?? [])].map(
                (view) => ({ value: view.urn, label: view.name }),
            ),
        [personalData, globalData],
    );
    const listsLoading = personalLoading || globalLoading;
    const selectedIsListed = !!value && listedOptions.some((option) => option.value === value);

    // The saved View may not be in the listed results (outside the page, or filtered out by the
    // search), so look it up directly to keep showing its name instead of an empty selection.
    const { data: selectedData, loading: selectedLoading } = useGetViewQuery({
        variables: { urn: value ?? '' },
        skip: !value || listsLoading || selectedIsListed,
        fetchPolicy: 'cache-first',
    });

    const options = useMemo(() => {
        if (!value || selectedIsListed || listsLoading || selectedLoading) return listedOptions;
        const label = selectedData?.view?.name ?? value;
        return [{ value, label }, ...listedOptions];
    }, [listedOptions, value, selectedIsListed, listsLoading, selectedLoading, selectedData]);

    return (
        <SimpleSelect
            options={options}
            values={value ? [value] : []}
            placeholder={
                listsLoading ? t('platform.ai.viewSelector.loading') : t('platform.ai.viewSelector.placeholder')
            }
            onUpdate={(values) => onChange(values[0] || undefined)}
            onClear={() => onChange(undefined)}
            showClear={!!value}
            showSearch
            filterResultsByQuery={false}
            onSearchChange={setSearchText}
            width="full"
            position="start"
        />
    );
};
