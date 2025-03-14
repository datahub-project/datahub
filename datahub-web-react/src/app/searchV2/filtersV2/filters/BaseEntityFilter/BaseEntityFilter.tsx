import { Select } from '@src/alchemy-components';
import { Entity, EntityType, FilterOperator } from '@src/types.generated';
import { debounce } from 'lodash';
import React, { useCallback, useState } from 'react';
import { FilterComponentProps } from '../../types';
import { DEBOUNCE_ON_SEARCH_TIMEOUT_MS } from '../constants';
import useValues from '../hooks/useValues';
import useOptions from './hooks/useOptions';
import { BaseEntitySelectOption } from './types';

interface BaseEntityFilterProps extends FilterComponentProps {
    filterName: string;
    renderEntity: (entity: Entity) => React.ReactNode;
    entityTypes: EntityType[];
}

export default function BaseEntityFilter({
    fieldName,
    filterName,
    appliedFilters,
    onUpdate,
    facetState,
    renderEntity,
    entityTypes,
}: BaseEntityFilterProps) {
    const [searchQuery, setSearchQuery] = useState<string>('');
    // used to forcibly show entities in options even though these entities are not in facet
    const [appliedEntities, setAppliedEntities] = useState<Entity[]>([]);
    const options = useOptions(appliedEntities, facetState, searchQuery, entityTypes, renderEntity);
    const values = useValues(appliedFilters);

    const filteringPredicate = (option: BaseEntitySelectOption, query: string) => {
        return option.displayName.toLocaleLowerCase().includes(query.toLocaleLowerCase());
    };

    const onSelectUpdate = useCallback(
        (newValues: string[]) => {
            const selectedOptions = options.filter((option) => newValues.includes(option.value));
            const selectedEntities = selectedOptions.map((option) => option.entity);

            onUpdate?.({
                filters: [
                    {
                        field: fieldName,
                        condition: FilterOperator.Equal,
                        values: newValues,
                    },
                ],
            });

            setAppliedEntities(selectedEntities);
        },
        [onUpdate, options, fieldName],
    );

    const onSearch = debounce((newQuery: string) => setSearchQuery(newQuery), DEBOUNCE_ON_SEARCH_TIMEOUT_MS);

    return (
        <Select<BaseEntitySelectOption>
            size="sm"
            values={values}
            onUpdate={onSelectUpdate}
            options={options}
            isMultiSelect
            showSearch
            selectLabelProps={{ variant: 'labeled', label: filterName }}
            width="fit-content"
            filteringPredicate={filteringPredicate}
            onSearch={onSearch}
        />
    );
}
