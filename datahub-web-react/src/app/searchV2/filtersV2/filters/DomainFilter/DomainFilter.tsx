import { NestedSelect } from '@src/alchemy-components/components/Select/Nested/NestedSelect';
import { SelectOption } from '@src/alchemy-components/components/Select/Nested/types';
import { Domain, EntityType, FilterOperator } from '@src/types.generated';
import React, { useMemo, useState } from 'react';
import { debounce } from 'lodash';
import { FilterComponentProps } from '../../types';
import { EntityIconWithName } from '../BaseEntityFilter/components/EntityIconWithName';
import useValues from '../hooks/useValues';
import useDomainsFromAggregations from './hooks/useDomainsFromAggregations';
import useDomainsFromSuggestions from './hooks/useDomainsFromSuggestions';
import useMergedDomains from './hooks/useMergedDomains';
import useOptionsFromDomains from './hooks/useOptionsFromDomains';
import { domainFilteringPredicate } from './utils';
import { DEBOUNCE_ON_SEARCH_TIMEOUT_MS } from '../constants';

export default function DomainFilter({ fieldName, facetState, appliedFilters, onUpdate }: FilterComponentProps) {
    const [entities, setEntities] = useState<Domain[]>([]);
    const [query, setQuery] = useState<string>('');
    const renderLabel = (domain: Domain) => <EntityIconWithName entity={domain} />;
    const values = useValues(appliedFilters);
    const domainsFromAggregations = useDomainsFromAggregations(facetState?.facet?.aggregations);
    const { domains: domainsFromSuggestions } = useDomainsFromSuggestions(query);
    const mergedDomains = useMergedDomains(entities, domainsFromAggregations, domainsFromSuggestions);
    const options = useOptionsFromDomains(mergedDomains, renderLabel);
    const initialValues = useMemo(() => options.filter((option) => values.includes(option.value)), [values, options]);

    const onSearch = debounce((newQuery: string) => setQuery(newQuery), DEBOUNCE_ON_SEARCH_TIMEOUT_MS);

    const onSelectUpdate = (selectedOptions: SelectOption[]) => {
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
    };

    return (
        <NestedSelect
            initialValues={initialValues}
            onUpdate={onSelectUpdate}
            onSearch={onSearch}
            options={options}
            shouldFilterOptions
            filteringPredicate={domainFilteringPredicate}
            isMultiSelect
            width="fit-content"
            showSearch
            showCount
            shouldManuallyUpdate
            shouldAlwaysSyncParentValues
            selectLabelProps={{ variant: 'labeled', label: 'Domains' }}
        />
    );
}
