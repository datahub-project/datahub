import { debounce } from 'lodash';
import React, { useCallback, useState } from 'react';

import useOptions from '@app/searchV2/filtersV2/filters/BaseEntityFilter/hooks/useOptions';
import { BaseEntitySelectOption } from '@app/searchV2/filtersV2/filters/BaseEntityFilter/types';
import { DEBOUNCE_ON_SEARCH_TIMEOUT_MS } from '@app/searchV2/filtersV2/filters/constants';
import useValues from '@app/searchV2/filtersV2/filters/hooks/useValues';
import { FilterComponentProps } from '@app/searchV2/filtersV2/types';
import { Select } from '@src/alchemy-components';
import { Entity, EntityType, FilterOperator } from '@src/types.generated';

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
    const options = useOptions(appliedEntities, facetState, searchQuery, entityTypes);
    const values = useValues(appliedFilters);

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

    if (options.length === 0) return null;

    return (
        <Select<BaseEntitySelectOption>
            size="sm"
            values={values}
            onUpdate={onSelectUpdate}
            options={options}
            isMultiSelect
            showSearch
            showClear
            selectLabelProps={{ variant: 'labeled', label: filterName }}
            renderCustomOptionText={(option) => renderEntity(option.entity)}
            width="fit-content"
            onSearchChange={onSearch}
        />
    );
}
