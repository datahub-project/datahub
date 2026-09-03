import { SimpleSelect } from '@components';
import React, { useCallback, useEffect, useMemo, useState } from 'react';
import styled from 'styled-components';

import { SelectOption } from '@components/components/Select/types';

import { FORBIDDEN_URN_CHARS_REGEX } from '@app/entity/shared/utils';
import useDebouncedCallback from '@app/shared/hooks/useDebouncedCallback';
import { useGetRecommendations } from '@app/shared/recommendation';

import { useGetAutoCompleteResultsLazyQuery } from '@graphql/search.generated';
import { Entity, EntityType, Tag } from '@types';

export const CREATE_TAG_VALUE = '____reserved____.createTagValue';

const CreateOptionLabel = styled.span`
    color: ${(props) => props.theme.colors.textBrand};
    font-weight: 500;
`;

// Validation pattern for tag names - no forbidden URN characters
const isValidTagName = (name: string): boolean => name.length > 0 && !FORBIDDEN_URN_CHARS_REGEX.test(name);

interface TagSelectOption extends SelectOption {
    color?: string | null;
}

type Props = {
    selectedUrns: string[];
    onUpdate: (urns: string[]) => void;
    renderOption: (option: SelectOption) => React.ReactNode;
    renderSelectedValue: (option: SelectOption) => React.ReactNode;
    placeholder?: string;
    width?: number | 'full' | 'fit-content';
    showSearch?: boolean;
    onSearchChange?: (query: string) => void;
    filterResultsByQuery?: boolean;
    dataTestId?: string;
    allowCreateTag?: boolean;
    onCreateTag?: (tagName: string) => void;
    existingUrns?: string[];
    /** Callback when entities are fetched from search/recommendations */
    onEntitiesFetched?: (entities: Entity[]) => void;
    /**
     * Pre-resolved entities for already-selected urns (e.g. from an existing filter being
     * edited). Seeds the entity cache so pre-selected values render labeled chips even
     * when absent from recommendations/search results.
     */
    defaultEntities?: Entity[];
};

/**
 * Reusable tag select component with built-in entity resolution and recommendations.
 * - Shows recommended tags initially (on first load)
 * - Shows search results when user types
 * - Maintains entity cache for displaying selected items
 * Caller provides renderOption and renderSelectedValue for custom rendering.
 */
export default function TagSelect({
    selectedUrns,
    onUpdate,
    renderOption,
    renderSelectedValue,
    placeholder = 'Search for tags...',
    width = 'full',
    showSearch = true,
    onSearchChange: externalOnSearchChange,
    filterResultsByQuery = false,
    dataTestId,
    allowCreateTag = false,
    onCreateTag,
    existingUrns = [],
    onEntitiesFetched,
    defaultEntities,
}: Props) {
    const [searchText, setSearchText] = useState('');
    const [entityCache, setEntityCache] = useState<Record<string, Entity>>(() =>
        Object.fromEntries((defaultEntities || []).map((entity) => [entity.urn, entity])),
    );

    // Fetch recommendations (initial state)
    const { recommendedData, loading: recommendationsLoading } = useGetRecommendations([EntityType.Tag]);

    // Fetch search results (when user types)
    const [autoComplete, { data: searchData, loading: searchLoading }] = useGetAutoCompleteResultsLazyQuery();

    // Only the network call is debounced — searchText updates synchronously since it
    // drives which option list (search vs recommendations) is shown while typing.
    const debouncedAutoComplete = useDebouncedCallback((query: string) => {
        autoComplete({
            variables: {
                input: {
                    type: EntityType.Tag,
                    query,
                    limit: 10,
                },
            },
        });
    });

    const handleSearch = useCallback(
        (text: string) => {
            const trimmed = text.trim();
            setSearchText(trimmed);
            if (trimmed.length > 0) {
                debouncedAutoComplete(trimmed);
            }
            externalOnSearchChange?.(text);
        },
        [debouncedAutoComplete, externalOnSearchChange],
    );

    // Extract entities from search results
    const searchEntities = useMemo<Entity[]>(
        () => (searchData?.autoComplete?.entities as Entity[] | undefined) || [],
        [searchData],
    );

    // Extract entities from recommendations
    const initialEntities = useMemo<Entity[]>(
        () => (!searchText ? (recommendedData as Entity[] | undefined) || [] : []),
        [searchText, recommendedData],
    );

    // Show search results when searching, recommendations initially
    const currentEntities = searchText ? searchEntities : initialEntities;

    // Update entity cache with all seen entities
    useEffect(() => {
        if (currentEntities.length === 0) return;
        const newEntities: Entity[] = [];
        setEntityCache((prev) => {
            const next = { ...prev };
            let changed = false;
            currentEntities.forEach((e) => {
                if (!next[e.urn]) {
                    next[e.urn] = e;
                    newEntities.push(e);
                    changed = true;
                }
            });
            return changed ? next : prev;
        });
        // Notify parent of newly fetched entities
        if (newEntities.length > 0) {
            onEntitiesFetched?.(newEntities);
        }
    }, [currentEntities, onEntitiesFetched]);

    // Merge default entities that arrive after mount (e.g. resolved asynchronously)
    useEffect(() => {
        if (!defaultEntities?.length) return;
        setEntityCache((prev) => {
            const next = { ...prev };
            let changed = false;
            defaultEntities.forEach((entity) => {
                if (!next[entity.urn]) {
                    next[entity.urn] = entity;
                    changed = true;
                }
            });
            return changed ? next : prev;
        });
    }, [defaultEntities]);

    // Also cache selected entities so they can render after search clears
    useEffect(() => {
        setEntityCache((prev) => {
            const next = { ...prev };
            let changed = false;
            selectedUrns.forEach((urn) => {
                if (!next[urn] && searchData?.autoComplete?.entities) {
                    const entity = searchData.autoComplete.entities.find((e) => (e as Entity).urn === urn);
                    if (entity) {
                        next[urn] = entity as Entity;
                        changed = true;
                    }
                }
            });
            return changed ? next : prev;
        });
    }, [selectedUrns, searchData]);

    // Build options from current entities, optionally including create option
    const options = useMemo<TagSelectOption[]>(() => {
        const existingSet = new Set(existingUrns);
        const baseOptions = currentEntities
            .filter((entity) => !existingSet.has(entity.urn))
            .map((entity) => {
                const tagEntity = entity as Tag;
                return {
                    value: entity.urn,
                    label: tagEntity.properties?.name || entity.urn,
                    color: tagEntity.properties?.colorHex,
                    ...entity,
                };
            });

        if (!allowCreateTag || !searchText.trim()) return baseOptions;

        const trimmed = searchText.trim();
        const exactMatch = baseOptions.some(
            (o) => typeof o.label === 'string' && o.label.toLowerCase() === trimmed.toLowerCase(),
        );

        // Only show create option if:
        // 1. No exact match exists
        // 2. Tag name is valid (no forbidden characters)
        // 3. No items already selected (create flow assigns exactly one new tag)
        const showCreate = !exactMatch && isValidTagName(trimmed) && selectedUrns.length === 0;

        if (!showCreate) return baseOptions;

        return [
            ...baseOptions,
            {
                value: CREATE_TAG_VALUE,
                label: `Create "${trimmed}"`,
            },
        ];
    }, [currentEntities, allowCreateTag, searchText, existingUrns, selectedUrns.length]);

    // Build combined options including selected items from cache
    const combinedOptions = useMemo<TagSelectOption[]>(() => {
        const map = new Map<string, TagSelectOption>();

        // Add current entities (search results or recommendations)
        options.forEach((opt) => {
            map.set(opt.value, opt);
        });

        // Add selected URNs from cache. Uncached urns still get a fallback option —
        // the select renders nothing for a value with no matching option, which would
        // make a pre-selected tag invisible and unremovable.
        selectedUrns.forEach((urn) => {
            if (map.has(urn)) return;
            const entity = entityCache[urn] as Tag | undefined;
            if (entity) {
                map.set(urn, {
                    value: urn,
                    label: entity.properties?.name || urn,
                    color: entity.properties?.colorHex,
                    ...entityCache[urn],
                });
            } else {
                // Tag urns embed the tag name: urn:li:tag:<name>
                map.set(urn, { value: urn, label: urn.replace('urn:li:tag:', '') });
            }
        });

        return Array.from(map.values());
    }, [options, selectedUrns, entityCache]);

    // Handle selection with create tag interception
    const handleUpdate = useCallback(
        (next: string[]) => {
            if (allowCreateTag && next.includes(CREATE_TAG_VALUE)) {
                onCreateTag?.(searchText.trim());
                return;
            }
            onUpdate(next);
        },
        [allowCreateTag, onCreateTag, searchText, onUpdate],
    );

    // Wrap renderOption to handle create option rendering
    const wrappedRenderOption = useCallback(
        (option: SelectOption) => {
            const content =
                option.value === CREATE_TAG_VALUE ? (
                    <CreateOptionLabel>{option.label}</CreateOptionLabel>
                ) : (
                    renderOption(option)
                );

            return <div data-testid={`tag-term-option-${option.label}`}>{content}</div>;
        },
        [renderOption],
    );

    const isLoading = searchLoading || recommendationsLoading;

    return (
        <SimpleSelect
            isMultiSelect
            showSearch={showSearch}
            onSearchChange={handleSearch}
            values={selectedUrns}
            onUpdate={handleUpdate}
            options={options}
            combinedSelectedAndSearchOptions={combinedOptions}
            renderCustomOptionText={wrappedRenderOption}
            renderCustomSelectedValue={renderSelectedValue}
            selectLabelProps={{ variant: 'custom' }}
            filterResultsByQuery={filterResultsByQuery}
            isLoading={isLoading}
            placeholder={placeholder}
            width={width}
            dataTestId={dataTestId}
        />
    );
}
