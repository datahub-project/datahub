import { Avatar, Text } from '@components';
import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import styled from 'styled-components';

import { OptionType } from '@components/components/AutoComplete/types';

import { deduplicateEntities, entitiesToSelectOptions } from '@app/entityV2/shared/utils/selectorUtils';
import { useGetRecommendations } from '@app/shared/recommendation';
import { addUserFiltersToAutoCompleteMultipleInput } from '@app/shared/userSearchUtils';
import { SimpleSelect } from '@src/alchemy-components/components/Select/SimpleSelect';
import EntityIcon from '@src/app/searchV2/autoCompleteV2/components/icon/EntityIcon';
import { useEntityRegistryV2 } from '@src/app/useEntityRegistry';

import { useGetAutoCompleteMultipleResultsLazyQuery } from '@graphql/search.generated';
import { CorpGroup, CorpUser, Entity, EntityType } from '@types';

const IconAndNameContainer = styled.div`
    display: flex;
    flex-direction: row;
    gap: 8px;
    align-items: center;
`;

const IconWrapper = styled.div`
    display: flex;
    align-items: center;

    & .ant-image {
        display: flex;
        align-items: center;
    }
`;

const TitleContainer = styled.div`
    display: flex;
    flex-direction: column;
`;

export interface ActorsSearchSelectProps {
    selectedActorUrns: string[];
    onUpdate: (selectedActors: (CorpUser | CorpGroup)[]) => void;
    placeholder?: string;
    defaultActors?: (CorpUser | CorpGroup)[];
    isDisabled?: boolean;
    width?: number | 'full' | 'fit-content';
    showSearch?: boolean;
}

/**
 * A specialized search and selection component for actors (CorpUser and CorpGroup entities).
 * Built on top of SimpleSelect with actor-specific features like recommendations,
 * avatar rendering, and placeholder support.
 *
 * TODO: Support resolving selected entities from selectedActorUrns on initial render.
 */
export const ActorsSearchSelect: React.FC<ActorsSearchSelectProps> = ({
    selectedActorUrns,
    onUpdate,
    placeholder = 'Search for users or groups',
    defaultActors: placeholderActors,
    isDisabled = false,
    width = 'full',
    showSearch = true,
}) => {
    const entityRegistry = useEntityRegistryV2();
    const [selectedActorEntities, setSelectedActorEntities] = useState<Entity[]>([]);
    const hasAutoSelectedRef = useRef(false);

    // Auto-select placeholder actors ONLY ONCE on initial render when no actors are selected
    useEffect(() => {
        if (
            placeholderActors &&
            placeholderActors.length > 0 &&
            !hasAutoSelectedRef.current &&
            selectedActorUrns.length === 0
        ) {
            onUpdate(placeholderActors);
            hasAutoSelectedRef.current = true;
        }
    }, [placeholderActors, selectedActorUrns.length, onUpdate]);

    // Autocomplete query for actors (CorpUser and CorpGroup types)
    const [autoCompleteQuery, { data: autocompleteData, loading: searchLoading }] =
        useGetAutoCompleteMultipleResultsLazyQuery({
            fetchPolicy: 'no-cache',
        });

    const { recommendedData, loading: recommendationsLoading } = useGetRecommendations([
        EntityType.CorpGroup,
        EntityType.CorpUser,
    ]);

    // Get results from the recommendations or autocomplete
    const searchResults: Array<Entity> = useMemo(() => {
        return (
            autocompleteData?.autoCompleteForMultiple?.suggestions?.flatMap((suggestion) => suggestion.entities) ||
            recommendedData ||
            []
        );
    }, [autocompleteData?.autoCompleteForMultiple?.suggestions, recommendedData]);

    // Sync selectedActorEntities with selectedActorUrns from parent
    useEffect(() => {
        const currentSelectedUrns = selectedActorEntities
            .map((e) => e.urn)
            .sort()
            .join(',');
        const newSelectedUrns = selectedActorUrns.sort().join(',');

        // Only update if the URNs have actually changed
        if (currentSelectedUrns !== newSelectedUrns) {
            const entities = selectedActorUrns
                .map((urn) => {
                    // Try to find in all available sources
                    return (
                        (placeholderActors || []).find((e) => e.urn === urn) ||
                        searchResults.find((e) => e.urn === urn) ||
                        selectedActorEntities.find((e) => e.urn === urn)
                    );
                })
                .filter(Boolean) as Entity[];

            setSelectedActorEntities(entities);
        }
    }, [selectedActorUrns, placeholderActors, searchResults, selectedActorEntities]);

    // Use utility to deduplicate entities from all sources
    const allOptionsEntities = deduplicateEntities({
        placeholderEntities: placeholderActors,
        searchResults,
        selectedEntities: selectedActorEntities,
    });

    // Convert entities to SelectOption format using utility
    const selectOptions = entitiesToSelectOptions(allOptionsEntities, entityRegistry);

    // Handle search
    const handleSearch = useCallback(
        (query: string) => {
            if (query.trim()) {
                const entityTypes = [EntityType.CorpUser, EntityType.CorpGroup];
                const input = addUserFiltersToAutoCompleteMultipleInput(
                    {
                        types: entityTypes,
                        query: query.trim(),
                        limit: 10,
                    },
                    entityTypes,
                );
                autoCompleteQuery({
                    variables: {
                        input,
                    },
                });
            }
        },
        [autoCompleteQuery],
    );

    // Render actor entity in dropdown
    const renderDropdownActorLabel = useCallback(
        (entity: Entity) => {
            const displayName = entityRegistry.getDisplayName(entity.type, entity);
            const subtitle = entity.type === EntityType.CorpUser ? (entity as any)?.properties?.email : undefined;
            return (
                <IconAndNameContainer>
                    <IconWrapper>
                        <EntityIcon entity={entity} />
                    </IconWrapper>
                    <TitleContainer>
                        <Text type="div">{displayName}</Text>
                        {subtitle && (
                            <Text type="div" size="sm" color="gray">
                                {subtitle}
                            </Text>
                        )}
                    </TitleContainer>
                </IconAndNameContainer>
            );
        },
        [entityRegistry],
    );

    // Render selected actor label
    const renderSelectedActorLabel = useCallback(
        (selectedOption: OptionType) => {
            const entity = allOptionsEntities.find((e) => e.urn === selectedOption.value) as
                | Partial<CorpUser>
                | Partial<CorpGroup>;
            if (!entity) return selectedOption.label;

            const displayName = entity.type
                ? entityRegistry.getDisplayName(entity.type, entity)
                : entity?.properties?.displayName;

            const imageUrl = entity.editableProperties?.pictureLink;
            return <Avatar name={displayName || ''} imageUrl={imageUrl} showInPill />;
        },
        [allOptionsEntities, entityRegistry],
    );

    // Handle select change
    const handleSelectChange = useCallback(
        (newValues: string[]) => {
            const newEntities = newValues
                .map((urn) => {
                    return (
                        allOptionsEntities.find((e) => e.urn === urn) ||
                        selectedActorEntities.find((e) => e.urn === urn)
                    );
                })
                .filter(Boolean) as (CorpUser | CorpGroup)[];

            setSelectedActorEntities(newEntities);
            onUpdate(newEntities);
        },
        [onUpdate, allOptionsEntities, selectedActorEntities],
    );

    // Loading state for the select
    const isSelectLoading = recommendationsLoading || searchLoading;

    return (
        <SimpleSelect
            selectLabelProps={{
                variant: 'custom',
            }}
            options={selectOptions}
            isLoading={isSelectLoading}
            values={selectedActorUrns}
            onUpdate={handleSelectChange}
            onSearchChange={handleSearch}
            showSearch={showSearch}
            isMultiSelect
            placeholder={placeholder}
            isDisabled={isDisabled}
            width={width}
            renderCustomSelectedValue={renderSelectedActorLabel}
            renderCustomOptionText={(option) => {
                const entity = allOptionsEntities.find((e) => e.urn === option.value);
                return entity ? renderDropdownActorLabel(entity) : option.label;
            }}
        />
    );
};
