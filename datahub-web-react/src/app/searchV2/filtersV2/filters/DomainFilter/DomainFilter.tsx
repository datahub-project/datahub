import { debounce } from 'lodash';
import React, { useMemo, useState } from 'react';

import { EntityIconWithName } from '@app/searchV2/filtersV2/filters/BaseEntityFilter/components/EntityIconWithName';
import useDomainsFromAggregations from '@app/searchV2/filtersV2/filters/DomainFilter/hooks/useDomainsFromAggregations';
import useDomainsFromSuggestions from '@app/searchV2/filtersV2/filters/DomainFilter/hooks/useDomainsFromSuggestions';
import useMergedDomains from '@app/searchV2/filtersV2/filters/DomainFilter/hooks/useMergedDomains';
import useOptionsFromDomains from '@app/searchV2/filtersV2/filters/DomainFilter/hooks/useOptionsFromDomains';
import { DEBOUNCE_ON_SEARCH_TIMEOUT_MS } from '@app/searchV2/filtersV2/filters/constants';
import useValues from '@app/searchV2/filtersV2/filters/hooks/useValues';
import { FilterComponentProps } from '@app/searchV2/filtersV2/types';
import { NestedSelect } from '@src/alchemy-components/components/Select/Nested/NestedSelect';
import { NestedSelectOption } from '@src/alchemy-components/components/Select/Nested/types';
import { Domain, EntityType, FilterOperator } from '@src/types.generated';

export default function DomainFilter({ fieldName, facetState, appliedFilters, onUpdate }: FilterComponentProps) {
    const [entities, setEntities] = useState<Domain[]>([]);
    const [query, setQuery] = useState<string>('');
    const values = useValues(appliedFilters);
    const domainsFromAggregations = useDomainsFromAggregations(facetState?.facet?.aggregations);
    const { domains: domainsFromSuggestions } = useDomainsFromSuggestions(query);
    const mergedDomains = useMergedDomains(entities, domainsFromAggregations, domainsFromSuggestions);
    const options = useOptionsFromDomains(mergedDomains);
    const initialValues = useMemo(() => options.filter((option) => values.includes(option.value)), [values, options]);

    const onSearch = debounce((newQuery: string) => setQuery(newQuery), DEBOUNCE_ON_SEARCH_TIMEOUT_MS);

    const onSelectUpdate = (selectedOptions: NestedSelectOption[]) => {
        if (facetState !== undefined) {
            const selectedValues = selectedOptions.map((option) => option.value);
            const selectedEntities: Domain[] = selectedOptions
                .map((option) => option.entity)
                .filter((entity): entity is Domain => !!entity && entity.type === EntityType.Domain);

            setEntities(selectedEntities);

            onUpdate?.({
                filters: [
                    {
                        field: fieldName,
                        condition: FilterOperator.Equal,
                        values: selectedValues,
                    },
                ],
            });
        }
    };

    return (
        <NestedSelect
            initialValues={initialValues}
            onUpdate={onSelectUpdate}
            onSearch={onSearch}
            options={options}
            renderCustomOptionText={(option) => <EntityIconWithName entity={option.entity} />}
            isMultiSelect
            width="fit-content"
            dataTestId="filter-domain"
            size="sm"
            showSearch
            showClear
            shouldDisplayConfirmationFooter
            shouldAlwaysSyncParentValues
            selectLabelProps={{ variant: 'labeled', label: 'Domains' }}
        />
    );
}
