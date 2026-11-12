import { SimpleSelect } from '@components';
import React, { useMemo } from 'react';
import { useTranslation } from 'react-i18next';

import { DEFAULT_LIST_VIEWS_PAGE_SIZE } from '@app/entityV2/view/utils';

import { useListGlobalViewsQuery, useListMyViewsQuery } from '@graphql/view.generated';
import { DataHubViewType } from '@types';

interface ViewSelectorProps {
    value: string | undefined;
    onChange: (viewUrn: string | undefined) => void;
}

/** Picks one of the viewer's personal Views or a global View. */
export const ViewSelector: React.FC<ViewSelectorProps> = ({ value, onChange }) => {
    const { t } = useTranslation('settings.ai');
    const { data: personalData, loading: personalLoading } = useListMyViewsQuery({
        variables: { start: 0, count: DEFAULT_LIST_VIEWS_PAGE_SIZE, viewType: DataHubViewType.Personal },
        fetchPolicy: 'cache-first',
    });
    const { data: globalData, loading: globalLoading } = useListGlobalViewsQuery({
        variables: { start: 0, count: DEFAULT_LIST_VIEWS_PAGE_SIZE },
        fetchPolicy: 'cache-first',
    });

    const options = useMemo(
        () =>
            [...(personalData?.listMyViews?.views ?? []), ...(globalData?.listGlobalViews?.views ?? [])].map(
                (view) => ({ value: view.urn, label: view.name }),
            ),
        [personalData, globalData],
    );
    const loading = personalLoading || globalLoading;

    return (
        <SimpleSelect
            options={options}
            values={value ? [value] : []}
            placeholder={loading ? t('platform.ai.viewSelector.loading') : t('platform.ai.viewSelector.placeholder')}
            onUpdate={(values) => onChange(values[0] || undefined)}
            onClear={() => onChange(undefined)}
            showClear={!!value}
            showSearch
            width="full"
            position="start"
        />
    );
};
