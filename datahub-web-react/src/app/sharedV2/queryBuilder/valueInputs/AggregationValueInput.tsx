import { Select, SelectOption } from '@components';
import React, { useCallback, useEffect, useMemo, useState } from 'react';

import { capitalizeFirstLetterOnly } from '@app/shared/textUtil';

import { useAggregateAcrossEntitiesLazyQuery } from '@graphql/search.generated';

type Props = {
    facetField: string;
    selectedValues: string[];
    mode?: 'multiple' | 'single';
    onChangeSelectedValues: (newValues: string[]) => void;
    label?: string;
    placeholder?: string;
};

export default function AggregationValueInput({
    facetField,
    selectedValues,
    mode,
    label,
    placeholder,
    onChangeSelectedValues,
}: Props) {
    const [searchQuery, setSearchQuery] = useState('');

    const [fetchAggregations, { data, loading }] = useAggregateAcrossEntitiesLazyQuery({
        fetchPolicy: 'cache-first',
    });

    useEffect(() => {
        fetchAggregations({
            variables: {
                input: {
                    query: '*',
                    facets: [facetField],
                },
            },
        });
    }, [facetField, fetchAggregations]);

    const aggregations = useMemo(
        () => data?.aggregateAcrossEntities?.facets?.find((f) => f.field === facetField)?.aggregations || [],
        [data, facetField],
    );

    const options: SelectOption[] = useMemo(() => {
        const aggOptions = aggregations.map((agg) => ({
            value: agg.value,
            label: capitalizeFirstLetterOnly(agg.value) || agg.value,
        }));

        const selectedSet = new Set(selectedValues);
        const existingValues = new Set(aggOptions.map((o) => o.value));

        const extraSelectedOptions = selectedValues
            .filter((v) => !existingValues.has(v))
            .map((v) => ({
                value: v,
                label: capitalizeFirstLetterOnly(v) || v,
            }));

        const allOptions = [...aggOptions, ...extraSelectedOptions];

        const filtered = searchQuery
            ? allOptions.filter((o) => o.label.toLowerCase().includes(searchQuery.toLowerCase()))
            : allOptions;

        return filtered.sort((a, b) => {
            const aSelected = selectedSet.has(a.value) ? 0 : 1;
            const bSelected = selectedSet.has(b.value) ? 0 : 1;
            return aSelected - bSelected;
        });
    }, [aggregations, selectedValues, searchQuery]);

    const onSearch = useCallback((text: string) => {
        setSearchQuery(text);
    }, []);

    const isMultiSelect = mode === 'multiple';

    return (
        <Select
            options={options}
            values={selectedValues}
            isMultiSelect={isMultiSelect}
            onUpdate={onChangeSelectedValues}
            onSearchChange={onSearch}
            placeholder={placeholder || 'Select a value...'}
            isLoading={loading}
            selectLabelProps={
                isMultiSelect && selectedValues.length > 0
                    ? {
                          variant: 'labeled',
                          label: label ?? 'Items',
                      }
                    : undefined
            }
            showClear
            width="full"
            showSearch
        />
    );
}
