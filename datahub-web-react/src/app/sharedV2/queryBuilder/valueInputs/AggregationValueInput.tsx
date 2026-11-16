import { Select, SelectOption } from '@components';
import React, { useCallback, useEffect, useMemo, useState } from 'react';
import { useTranslation } from 'react-i18next';

import { capitalizeFirstLetterOnly } from '@app/sharedV2/textUtil';
import { useGetEntities } from '@app/sharedV2/useGetEntities';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { useAggregateAcrossEntitiesLazyQuery } from '@graphql/search.generated';
import { DataPlatformInstance, Entity, EntityType } from '@types';

// The backend default (20) silently hides values past the top-N by count, and search only filters
// client-side, so ask for enough to cover every platform/instance. The backend clamps this to its
// own max term bucket size.
const MAX_AGGREGATION_VALUES = 1000;

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
    const { t } = useTranslation('shared.query-builder');
    const entityRegistry = useEntityRegistry();
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
                    searchFlags: { maxAggValues: MAX_AGGREGATION_VALUES },
                },
            },
        });
    }, [facetField, fetchAggregations]);

    const aggregations = useMemo(
        () => data?.aggregateAcrossEntities?.facets?.find((f) => f.field === facetField)?.aggregations || [],
        [data, facetField],
    );

    // Selected values can be missing from the facet (e.g. a saved View whose platform no longer has
    // assets); resolve those URNs so they don't render raw.
    const missingSelectedUrns = useMemo(() => {
        if (loading) return [];
        const aggValues = new Set(aggregations.map((agg) => agg.value));
        return selectedValues.filter((v) => !aggValues.has(v));
    }, [aggregations, selectedValues, loading]);
    const { entities: missingSelectedEntities } = useGetEntities(missingSelectedUrns, false, {
        skipLineage: true,
        skipSiblingsSearch: true,
    });

    const getEntityLabel = useCallback(
        (entity: Entity) => {
            const name = entityRegistry.getDisplayName(entity.type, entity);
            // Instance IDs like `prod` repeat across platforms; prefix the platform so they're distinguishable.
            const platform =
                entity.type === EntityType.DataPlatformInstance && (entity as DataPlatformInstance).platform;
            return platform ? `${entityRegistry.getDisplayName(EntityType.DataPlatform, platform)} ${name}` : name;
        },
        [entityRegistry],
    );

    const options: SelectOption[] = useMemo(() => {
        const aggOptions = aggregations.map((agg) => ({
            value: agg.value,
            // Entity-backed facets (platform, platformInstance, …) aggregate on URNs; resolve those
            // to the entity's display name so the dropdown isn't a list of raw URNs.
            label: agg.entity ? getEntityLabel(agg.entity) : capitalizeFirstLetterOnly(agg.value) || agg.value,
        }));

        const selectedSet = new Set(selectedValues);
        const existingValues = new Set(aggOptions.map((o) => o.value));

        const extraSelectedOptions = selectedValues
            .filter((v) => !existingValues.has(v))
            .map((v) => {
                const entity = missingSelectedEntities.find((e) => e.urn === v);
                return { value: v, label: entity ? getEntityLabel(entity) : capitalizeFirstLetterOnly(v) || v };
            });

        const allOptions = [...aggOptions, ...extraSelectedOptions];

        const filtered = searchQuery
            ? allOptions.filter((o) => o.label.toLowerCase().includes(searchQuery.toLowerCase()))
            : allOptions;

        return filtered.sort((a, b) => {
            const aSelected = selectedSet.has(a.value) ? 0 : 1;
            const bSelected = selectedSet.has(b.value) ? 0 : 1;
            return aSelected - bSelected;
        });
    }, [aggregations, selectedValues, searchQuery, getEntityLabel, missingSelectedEntities]);

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
            placeholder={placeholder || t('value.defaultPlaceholder')}
            isLoading={loading}
            selectLabelProps={
                isMultiSelect && selectedValues.length > 0
                    ? {
                          variant: 'labeled',
                          label: label ?? t('value.items'),
                      }
                    : undefined
            }
            showClear
            width="full"
            showSearch
        />
    );
}
