import { SearchBar } from '@components';
import React, { useMemo, useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { ShowMoreSection } from '@app/entityV2/shared/sidebarSection/ShowMoreSection';
import StructuredPropertyValue from '@app/entityV2/shared/tabs/Properties/StructuredPropertyValue';
import { PropertyRow, ValueColumnData } from '@app/entityV2/shared/tabs/Properties/types';
import { useHydratedEntityMap } from '@app/entityV2/shared/tabs/Properties/useHydratedEntityMap';
import EntityRegistry from '@src/app/entityV2/EntityRegistry';
import { useEntityRegistryV2 } from '@src/app/useEntityRegistry';

/**
 * How many values of one property render before the user has to ask for more. Matches the other
 * sidebar sections (owners, groups), which show a handful and then "Show N more".
 */
export const DEFAULT_MAX_VALUES_TO_SHOW = 5;

const FilterContainer = styled.div`
    width: 100%;
    margin-bottom: 8px;
`;

interface Props {
    propertyRow: PropertyRow;
    isRichText?: boolean;
    filterText?: string;
    maxValuesToShow?: number;
    /** Wraps each rendered value; the Properties tab uses it for layout. */
    renderValue: (value: ValueColumnData, node: React.ReactNode) => React.ReactNode;
    dataTestId?: (value: ValueColumnData) => string;
}

/** The URN a value refers to, whether it arrived resolved (`entity`) or as a bare URN string. */
export function valueUrn(value: ValueColumnData): string | undefined {
    if (value.entity?.urn) return value.entity.urn;
    return typeof value.value === 'string' && value.value.startsWith('urn:li:') ? value.value : undefined;
}

/** Case-insensitive match on the raw value or, for entity values, the entity's display name. */
export function valueMatches(value: ValueColumnData, query: string, entityRegistry: EntityRegistry): boolean {
    const needle = query.trim().toLocaleLowerCase();
    if (!needle) return true;
    if ((value.value?.toString() ?? '').toLocaleLowerCase().includes(needle)) return true;
    if (!value.entity) return false;
    return entityRegistry.getDisplayName(value.entity.type, value.entity).toLocaleLowerCase().includes(needle);
}

/**
 * The values that survive both filters: the list's own filter box and the page-level search
 * (`filterText`, the Properties tab's "Search in properties"). The page-level search decides which
 * property rows are shown by matching any value, so it has to narrow the list too, or a hit past
 * the first page would show a row with no visible match.
 */
export function selectVisibleValues(
    values: ValueColumnData[],
    query: string,
    filterText: string | undefined,
    entityRegistry: EntityRegistry,
): ValueColumnData[] {
    return values.filter(
        (value) =>
            valueMatches(value, query, entityRegistry) &&
            (!filterText || valueMatches(value, filterText, entityRegistry)),
    );
}

/**
 * Renders a structured property's values a page at a time and hydrates only the entities that are
 * on screen. Filter and paging state live in this component, so parents key it by property AND
 * entity: the same property appears on many entities and React would otherwise carry one entity's
 * filter over to the next. Whenever there is more than one page, a filter box appears above the list so a specific
 * value can be found by typing instead of paging. A property can carry thousands of URN values;
 * rendering and hydrating all of them at once is what made entity pages with such properties take
 * tens of seconds to load.
 */
export default function StructuredPropertyValueList({
    propertyRow,
    isRichText,
    filterText,
    maxValuesToShow = DEFAULT_MAX_VALUES_TO_SHOW,
    renderValue,
    dataTestId,
}: Props) {
    const { t } = useTranslation('entity.profile.tabs');
    const entityRegistry = useEntityRegistryV2();
    const values = useMemo(() => propertyRow.values ?? [], [propertyRow.values]);
    const [query, setQuery] = useState('');
    const [shownCount, setShownCount] = useState(maxValuesToShow);

    const filteredValues = useMemo(
        () => selectVisibleValues(values, query, filterText, entityRegistry),
        [values, query, filterText, entityRegistry],
    );
    const shownValues = useMemo(() => filteredValues.slice(0, shownCount), [filteredValues, shownCount]);
    const urnsToHydrate = useMemo(() => shownValues.map(valueUrn), [shownValues]);
    const hydratedEntityMap = useHydratedEntityMap(urnsToHydrate);

    return (
        <>
            {values.length > maxValuesToShow && (
                <FilterContainer>
                    <SearchBar
                        value={query}
                        onChange={(next) => {
                            setQuery(next);
                            setShownCount(maxValuesToShow);
                        }}
                        debounceDelay={150}
                        data-testid={`property-${propertyRow.displayName}-values-filter`}
                    />
                </FilterContainer>
            )}
            {shownValues.map((value) =>
                renderValue(
                    value,
                    <StructuredPropertyValue
                        value={value}
                        isRichText={isRichText}
                        filterText={filterText}
                        hydratedEntityMap={hydratedEntityMap}
                        attribution={propertyRow.attribution}
                        dataTestId={dataTestId?.(value)}
                    />,
                ),
            )}
            {filteredValues.length > shownCount && (
                <ShowMoreSection
                    totalCount={filteredValues.length}
                    entityCount={shownCount}
                    setEntityCount={setShownCount}
                    showMaxEntity={maxValuesToShow}
                    moreLabel={(count) => t('properties.showCountMoreValues', { count, total: filteredValues.length })}
                />
            )}
        </>
    );
}
