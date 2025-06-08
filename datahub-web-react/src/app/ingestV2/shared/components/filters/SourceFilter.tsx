import { Select } from '@components';
import React, { useCallback, useEffect, useState } from 'react';

import { CLI_EXECUTOR_ID } from '@app/ingest/source/utils';
import useIngestionSourcesFromData from '@app/ingestV2/shared/components/filters/hooks/useIngestionSourcesFromData';
import useOptions from '@app/ingestV2/shared/components/filters/hooks/useOptions';
import { NameColumn } from '@app/ingestV2/source/IngestionSourceTableColumns';
import { getIngestionSourceSystemFilter } from '@app/ingestV2/source/utils';
import { MIN_CHARACTER_COUNT_FOR_SEARCH } from '@app/searchV2/utils/constants';

import { useListIngestionSourcesLazyQuery } from '@graphql/ingestion.generated';
import { IngestionSource } from '@types';

const COUNT_OF_INGESTION_SOURCES_TO_FETCH = 10;

interface Props {
    defaultValues?: string[];
    onUpdate?: (selectedValues: string[]) => void;
    hideSystemSources: boolean;
    shouldPreserveParams: React.MutableRefObject<boolean>;
}

export default function SourceFilter({ defaultValues, onUpdate, hideSystemSources, shouldPreserveParams }: Props) {
    const [values, setValues] = useState<string[]>(defaultValues ?? []);
    const [query, setQuery] = useState<string | undefined>();

    const [getListIngestionSources, { data, loading }] = useListIngestionSourcesLazyQuery();
    const [getListIngestionSourcesByDefaultValues, { data: defaultData, loading: loadingDefault }] =
        useListIngestionSourcesLazyQuery();

    useEffect(() => {
        if (shouldPreserveParams.current) {
            setValues(defaultValues || []);
        }
    }, [defaultValues, shouldPreserveParams]);

    // Fething of items by defaultValues
    useEffect(() => {
        if (defaultValues) {
            getListIngestionSourcesByDefaultValues({
                variables: {
                    input: {
                        start: 0,
                        count: defaultValues.length,
                        filters: [
                            { field: 'urn', values: defaultValues },
                            getIngestionSourceSystemFilter(hideSystemSources),
                        ],
                    },
                },
            });
        }
    }, [defaultValues, getListIngestionSourcesByDefaultValues, hideSystemSources]);

    // Fetching of items by query
    useEffect(() => {
        getListIngestionSources({
            variables: {
                input: {
                    start: 0,
                    count: COUNT_OF_INGESTION_SOURCES_TO_FETCH,
                    query: query ?? '*',
                    filters: [getIngestionSourceSystemFilter(hideSystemSources)],
                },
            },
        });
    }, [query, getListIngestionSources, hideSystemSources]);

    const defaultItems = useIngestionSourcesFromData(defaultData, loadingDefault);
    const items = useIngestionSourcesFromData(data, loading);

    const itemToOption = useCallback(
        (item: IngestionSource) => ({
            label: item.name,
            value: item.urn,
            item,
        }),
        [],
    );
    const { options, onSelectedValuesChanged } = useOptions<IngestionSource>(defaultItems, items, itemToOption);

    const onUpdateHandler = useCallback(
        (selectedValues: string[]) => {
            setValues(selectedValues);
            onSelectedValuesChanged(selectedValues);
            onUpdate?.(selectedValues);
        },
        [onUpdate, onSelectedValuesChanged],
    );

    const onSearchChangeHandler = useCallback((searchText: string) => {
        if (searchText.length < MIN_CHARACTER_COUNT_FOR_SEARCH) {
            // don't invoke search queries, but filtering in select will still work
            setQuery(undefined);
        } else {
            setQuery(searchText);
        }
    }, []);

    return (
        <Select
            values={values}
            onUpdate={onUpdateHandler}
            options={options}
            isMultiSelect
            selectLabelProps={{ variant: 'labeled', label: 'Name' }}
            renderCustomOptionText={(option) => (
                <NameColumn
                    type={option.item.type}
                    record={{
                        name: option.item.name,
                        cliIngestion: option.item.config.executorId === CLI_EXECUTOR_ID,
                    }}
                />
            )}
            showSearch
            onSearchChange={onSearchChangeHandler}
            size="sm"
            width="fit-content"
        />
    );
}
