import { Select, Text } from '@components';
import React, { useMemo } from 'react';
import { useTranslation } from 'react-i18next';

import { NameColumn } from '@app/ingestV2/source/IngestionSourceTableColumns';
import { useIngestionSources } from '@app/ingestV2/source/builder/useIngestionSources';
import { getIngestionSourceSystemFilter } from '@app/ingestV2/source/utils';
import { capitalizeFirstLetter } from '@app/shared/textUtil';

import { useListIngestionSourceTypeFacetsQuery } from '@graphql/ingestion.generated';

interface Props {
    values: string[];
    onUpdate: (selectedValues: string[]) => void;
    hideSystemSources: boolean;
}

export default function SourceTypeFilter({ values, onUpdate, hideSystemSources }: Props) {
    const { t } = useTranslation('ingestion');
    const { t: tf } = useTranslation('common.feedback');
    const { ingestionSources } = useIngestionSources();

    const displayNameByType = useMemo(
        () => new Map(ingestionSources.map((source) => [source.name, source.displayName])),
        [ingestionSources],
    );

    const { data, loading } = useListIngestionSourceTypeFacetsQuery({
        variables: {
            input: {
                start: 0,
                count: 0,
                filters: [getIngestionSourceSystemFilter(hideSystemSources)],
            },
        },
        fetchPolicy: 'cache-and-network',
    });

    const isInitialLoading = loading && !data;
    const typeFacet = data?.listIngestionSources?.facets?.find((facet) => facet.field === 'type');

    const options = useMemo(
        () =>
            (typeFacet?.aggregations ?? [])
                .filter((aggregation) => !!aggregation.value)
                .map((aggregation) => ({
                    label:
                        displayNameByType.get(aggregation.value) ??
                        capitalizeFirstLetter(aggregation.value) ??
                        aggregation.value,
                    value: aggregation.value,
                })),
        [typeFacet, displayNameByType],
    );

    return (
        <Select
            values={values}
            onUpdate={onUpdate}
            options={options}
            isMultiSelect
            emptyState={isInitialLoading ? <Text color="gray">{tf('loading')}</Text> : undefined}
            selectLabelProps={{ variant: 'labeled', label: t('filters.sourceType') }}
            renderCustomOptionText={(option) => <NameColumn type={option.value} record={{ name: option.label }} />}
            showSearch
            size="sm"
            width="fit-content"
            data-testid="ingestion-source-type-filter"
        />
    );
}
