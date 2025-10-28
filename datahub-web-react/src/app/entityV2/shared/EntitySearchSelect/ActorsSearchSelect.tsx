import { Avatar, Text } from '@components';
import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import styled from 'styled-components';

import { OptionType } from '@components/components/AutoComplete/types';

import { deduplicateEntities, entitiesToSelectOptions } from '@app/entityV2/shared/utils/selectorUtils';
import { useGetRecommendations } from '@app/shared/recommendation';
import { SimpleSelect } from '@src/alchemy-components/components/Select/SimpleSelect';
import EntityIcon from '@src/app/searchV2/autoCompleteV2/components/icon/EntityIcon';
import { useEntityRegistryV2 } from '@src/app/useEntityRegistry';
import { 
    ActorEntity,
    filterActors, 
    resolveActorsFromUrns,
    getActorEmail,
    getActorPictureLink 
} from '@app/entityV2/shared/utils/actorUtils';

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
    onUpdate: (selectedActors: ActorEntity[]) => void;
    placeholder?: string;
    defaultActors?: ActorEntity[];
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
    const [selectedActorEntities, setSelectedActorEntities] = useState<ActorEntity[]>([]);
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

    // Filter search results to only include actors
    const actorSearchResults = useMemo(() => {
        return filterActors(searchResults);
    }, [searchResults]);

    // Sync selectedActorEntities with selectedActorUrns from parent
    useEffect(() => {
        const currentSelectedUrns = selectedActorEntities
            .map((e) => e.urn)
            .sort()
            .join(',');
        const newSelectedUrns = selectedActorUrns.sort().join(',');

        // Only update if the URNs have actually changed
        if (currentSelectedUrns !== newSelectedUrns) {
            const entities = resolveActorsFromUrns(selectedActorUrns, {
                placeholderActors,
                searchResults,
                selectedActors: selectedActorEntities
            });

            setSelectedActorEntities(entities);
        }
    }, [selectedActorUrns, placeholderActors, searchResults, selectedActorEntities]);

    // Use utility to deduplicate entities from all sources
    // Only include actor entities in the options
    const allActorEntities = useMemo(() => {
        const combined = deduplicateEntities({
            placeholderEntities: placeholderActors,
            searchResults: actorSearchResults,
            selectedEntities: selectedActorEntities,
        });
        return filterActors(combined);
    }, [placeholderActors, actorSearchResults, selectedActorEntities]);

    // Convert entities to SelectOption format using utility
    const selectOptions = entitiesToSelectOptions(allActorEntities, entityRegistry);

    // Handle search
    const handleSearch = useCallback(
        (query: string) => {
            if (query.trim()) {
                autoCompleteQuery({
                    variables: {
                        input: {
                            types: [EntityType.CorpUser, EntityType.CorpGroup],
                            query: query.trim(),
                            limit: 10,
                        },
                    },
                });
            }
        },
        [autoCompleteQuery],
    );

    // Render actor entity in dropdown
    const renderDropdownActorLabel = useCallback(
        (entity: ActorEntity) => {
            const displayName = entityRegistry.getDisplayName(entity.type, entity);
            const subtitle = getActorEmail(entity);
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
            const entity = allActorEntities.find((e) => e.urn === selectedOption.value);
            if (!entity) return selectedOption.label;

            const displayName = entityRegistry.getDisplayName(entity.type, entity);
            const imageUrl = getActorPictureLink(entity);
            
            return <Avatar name={displayName || ''} imageUrl={imageUrl} showInPill />;
        },
        [allActorEntities, entityRegistry],
    );

    // Handle select change
    const handleSelectChange = useCallback(
        (newValues: string[]) => {
            const newEntities = resolveActorsFromUrns(newValues, {
                placeholderActors: allActorEntities,
                selectedActors: selectedActorEntities
            });

            setSelectedActorEntities(newEntities);
            onUpdate(newEntities);
        },
        [onUpdate, allActorEntities, selectedActorEntities],
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
                const entity = allActorEntities.find((e) => e.urn === option.value);
                return entity ? renderDropdownActorLabel(entity) : option.label;
            }}
        />
    );
};
