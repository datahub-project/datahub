import { debounce, differenceBy, uniqBy } from 'lodash';
import { CaretDown } from 'phosphor-react';
import React, { useCallback, useEffect, useRef, useState } from 'react';
import styled from 'styled-components';

import SearchFiltersLoadingSection from '@app/searchV2/filters/SearchFiltersLoadingSection';
import { formatNumber } from '@app/shared/formatNumber';
import { FilterLabel } from '@app/sharedV2/filters/Filter';
import EntitySelectDropdown from '@app/taskCenterV2/proposalsV2/EntitySelectDropdown';
import { PROPOSAL_TARGET_ENTITY_TYPES } from '@app/taskCenterV2/proposalsV2/utils';
import { Icon, Pill } from '@src/alchemy-components';
import { useEntityRegistryV2 } from '@src/app/useEntityRegistry';
import { useGetSearchResultsForMultipleLazyQuery } from '@src/graphql/search.generated';
import { Entity } from '@src/types.generated';

const Container = styled.div``;
interface EntitySelectorProps {
    defaultSuggestions: Entity[] | undefined;
    defaultSuggestionsLoading: boolean;
    onUpdate?: (selectedValues: string[]) => void;
    selected?: string[];
    fetchedSelectedEntityData?: Entity[];
    loading?: boolean;
}

export const ProposalsEntitySelect = ({
    onUpdate,
    selected,
    defaultSuggestions = [],
    defaultSuggestionsLoading,
    fetchedSelectedEntityData = [],
    loading,
}: EntitySelectorProps) => {
    const entityRegistry = useEntityRegistryV2();
    const [selectedEntityUrns, setSelectedEntityUrns] = useState<any>(selected || []);
    const [selectedEntities, setSelectedEntities] = useState<Entity[]>([]);
    const [validDefaultSuggestions, setValidDefaultSuggestions] = useState<Entity[]>([]);
    const [cachedSuggestions, setCachedSuggestions] = useState<Entity[]>([]);

    // Search results
    const [searchAcrossEntities, { data: searchData, loading: searchResultsLoading }] =
        useGetSearchResultsForMultipleLazyQuery();
    const searchSuggestions: Entity[] =
        searchData?.searchAcrossEntities?.searchResults?.flatMap((result) => result.entity) || [];

    useEffect(() => {
        if (defaultSuggestions.length) {
            setValidDefaultSuggestions(defaultSuggestions);
        }
    }, [defaultSuggestions]);

    const [useSearch, setUseSearch] = useState(false);
    const suggestions: Entity[] = useSearch
        ? searchSuggestions
        : [...cachedSuggestions, ...differenceBy(validDefaultSuggestions, cachedSuggestions, 'urn')];

    // Set local state initially based on fetchedSelectedEntityData
    const synced = useRef(false);
    useEffect(() => {
        if (fetchedSelectedEntityData.length && !synced.current) {
            synced.current = true;
            setSelectedEntityUrns(fetchedSelectedEntityData.map((e) => e.urn));
            setSelectedEntities(fetchedSelectedEntityData);
            setCachedSuggestions(fetchedSelectedEntityData);
        }
    }, [fetchedSelectedEntityData]);

    // Prepare Options
    const availableEntityOptions = uniqBy(
        suggestions?.map((entity) => ({
            value: entity.urn,
            label: entityRegistry.getDisplayName(entity.type, entity),
            entity,
        })) || [],
        'value',
    );

    const handleUpdate = (values: string[]) => {
        const newSelectedEntities: Entity[] = [];
        values.forEach((value) => {
            const entity =
                suggestions.find((sug) => sug?.urn === value) ||
                searchSuggestions.find((sug) => sug.urn === value) ||
                selectedEntities.find((sug) => sug.urn === value);

            if (entity) {
                newSelectedEntities.push(entity);
            }
        });

        setSelectedEntities(newSelectedEntities);
        setSelectedEntityUrns(newSelectedEntities.map((e) => e.urn));
        onUpdate?.(newSelectedEntities.map((e) => e.urn));
    };

    const handleSearch = (text: string) => {
        if (text) {
            searchAcrossEntities({
                variables: {
                    input: {
                        types: PROPOSAL_TARGET_ENTITY_TYPES,
                        query: text,
                        start: 0,
                        count: 10,
                    },
                },
            });
            setUseSearch(true);
        } else {
            setUseSearch(false);
        }
    };

    const handleClose = useCallback(() => {
        setCachedSuggestions(selectedEntities);
    }, [selectedEntities]);

    return (
        <Container>
            <EntitySelectDropdown
                options={availableEntityOptions}
                values={selectedEntityUrns}
                onSearchChange={debounce(handleSearch, 200)}
                onUpdate={handleUpdate}
                onClose={handleClose}
                isLoading={defaultSuggestionsLoading || searchResultsLoading}
            >
                {loading && !availableEntityOptions?.length ? (
                    <SearchFiltersLoadingSection noOfLoadingSkeletons={1} />
                ) : (
                    <FilterLabel $isActive={!!selectedEntityUrns.length} data-testid="filter-dropdown-entity-select">
                        Entity
                        {!!selectedEntityUrns.length && (
                            <Pill size="xs" label={formatNumber(selectedEntityUrns.length)} />
                        )}
                        {!!selectedEntityUrns.length && (
                            <Icon
                                source="phosphor"
                                icon="X"
                                size="sm"
                                onClick={(e) => {
                                    e.stopPropagation();
                                    handleUpdate([]);
                                }}
                            />
                        )}
                        <CaretDown style={{ fontSize: '14px', height: '14px' }} />
                    </FilterLabel>
                )}
            </EntitySelectDropdown>
        </Container>
    );
};
