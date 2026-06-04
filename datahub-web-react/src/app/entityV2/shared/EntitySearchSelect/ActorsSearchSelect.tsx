import { Avatar, Text } from '@components';
import React, { useCallback, useEffect, useMemo, useState } from 'react';
import { useDebounce } from 'react-use';
import styled from 'styled-components';

import { OptionType } from '@components/components/AutoComplete/types';
import { AvatarType } from '@components/components/AvatarStack/types';

import { useActorOptions } from '@app/entityV2/shared/EntitySearchSelect/useActorOptions';
import {
    ActorEntity,
    getActorEmail,
    getActorPictureLink,
    resolveActorsFromUrns,
} from '@app/entityV2/shared/utils/actorUtils';
import { entitiesToSelectOptions } from '@app/entityV2/shared/utils/selectorUtils';
import { DEBOUNCE_SEARCH_MS } from '@app/shared/constants';
import { addUserFiltersToAutoCompleteMultipleInput } from '@app/shared/userSearchUtils';
import { SimpleSelect } from '@src/alchemy-components/components/Select/SimpleSelect';
import EntityIcon from '@src/app/searchV2/autoCompleteV2/components/icon/EntityIcon';
import { useEntityRegistryV2 } from '@src/app/useEntityRegistry';

import { useGetAutoCompleteMultipleResultsLazyQuery } from '@graphql/search.generated';
import { Entity, EntityType } from '@types';

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

interface ActorsSearchSelectProps {
    selectedActorUrns: string[];
    onUpdate: (selectedActors: ActorEntity[]) => void;
    placeholder?: string;
    label?: string;
    entityUrn?: string;
    includeCurrentUser?: boolean;
    includeCurrentUserGroups?: boolean;
    isDisabled?: boolean;
    isLoading?: boolean;
    width?: number | 'full' | 'fit-content';
    showSearch?: boolean;
    entityTypes?: EntityType[];
    dataTestId?: string;
}

/**
 * A specialized search and selection component for actors (CorpUser and CorpGroup entities).
 * Built on top of SimpleSelect with actor-specific features like recommendations,
 * avatar rendering, selected actor hydration, and contextual empty-state options.
 */
const DEFAULT_ACTOR_TYPES = [EntityType.CorpUser, EntityType.CorpGroup];

export const ActorsSearchSelect: React.FC<ActorsSearchSelectProps> = ({
    selectedActorUrns,
    onUpdate,
    placeholder = 'Search for users or groups',
    label,
    entityUrn,
    includeCurrentUser = true,
    includeCurrentUserGroups = true,
    isDisabled = false,
    isLoading = false,
    width = 'full',
    showSearch = true,
    entityTypes = DEFAULT_ACTOR_TYPES,
    dataTestId,
}) => {
    const entityRegistry = useEntityRegistryV2();
    const [searchQuery, setSearchQuery] = useState('');
    const [debouncedSearchQuery, setDebouncedSearchQuery] = useState('');

    // Autocomplete query for actors (CorpUser and CorpGroup types)
    const [autoCompleteQuery, { data: autocompleteData, loading: searchLoading }] =
        useGetAutoCompleteMultipleResultsLazyQuery({
            fetchPolicy: 'no-cache',
        });

    useDebounce(
        () => {
            setDebouncedSearchQuery(searchQuery.trim());
        },
        DEBOUNCE_SEARCH_MS,
        [searchQuery],
    );

    useEffect(() => {
        if (!debouncedSearchQuery) return;
        autoCompleteQuery({
            variables: {
                input: addUserFiltersToAutoCompleteMultipleInput(
                    {
                        types: entityTypes,
                        query: debouncedSearchQuery,
                        limit: 10,
                    },
                    entityTypes,
                ),
            },
        });
    }, [autoCompleteQuery, debouncedSearchQuery, entityTypes]);

    const typedSearchResults: Array<Entity> = useMemo(() => {
        const autocomplete = autocompleteData?.autoCompleteForMultiple;
        if (!debouncedSearchQuery || autocomplete?.query !== debouncedSearchQuery) return [];
        return autocomplete?.suggestions?.flatMap((suggestion) => suggestion.entities) || [];
    }, [autocompleteData?.autoCompleteForMultiple, debouncedSearchQuery]);

    const isSearching = !!searchQuery.trim();
    const {
        actorOptions: dropdownActorEntities,
        selectedActors,
        loading: actorOptionsLoading,
    } = useActorOptions({
        selectedActorUrns,
        entityTypes,
        entityUrn,
        includeCurrentUser,
        includeCurrentUserGroups,
        searchResults: typedSearchResults,
        isSearching,
    });

    const selectedAndDropdownActorEntities = useMemo(() => {
        const seenUrns = new Set<string>();
        return [...selectedActors, ...dropdownActorEntities].filter((actor) => {
            if (seenUrns.has(actor.urn)) return false;
            seenUrns.add(actor.urn);
            return true;
        });
    }, [dropdownActorEntities, selectedActors]);

    // Convert entities to SelectOption format using utility
    const selectOptions = useMemo(
        () => entitiesToSelectOptions(dropdownActorEntities, entityRegistry),
        [dropdownActorEntities, entityRegistry],
    );

    const selectedAndDropdownOptions = useMemo(
        () => entitiesToSelectOptions(selectedAndDropdownActorEntities, entityRegistry),
        [selectedAndDropdownActorEntities, entityRegistry],
    );

    // Handle search
    const handleSearch = useCallback((query: string) => {
        setSearchQuery(query);
    }, []);

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
            const entity = selectedAndDropdownActorEntities.find((e) => e.urn === selectedOption.value);
            if (!entity) return selectedOption.label;

            const displayName = entityRegistry.getDisplayName(entity.type, entity);
            const imageUrl = getActorPictureLink(entity);
            const avatarType = entity.type === EntityType.CorpGroup ? AvatarType.group : AvatarType.user;

            return <Avatar name={displayName || ''} imageUrl={imageUrl} type={avatarType} showInPill />;
        },
        [selectedAndDropdownActorEntities, entityRegistry],
    );

    // Handle select change
    const handleSelectChange = useCallback(
        (newValues: string[]) => {
            const newEntities = resolveActorsFromUrns(newValues, {
                placeholderActors: selectedAndDropdownActorEntities,
            });

            onUpdate(newEntities);
        },
        [onUpdate, selectedAndDropdownActorEntities],
    );

    // Loading state for the select
    const isSelectLoading = isLoading || actorOptionsLoading || (isSearching && searchLoading);

    return (
        <SimpleSelect
            selectLabelProps={{
                variant: 'custom',
            }}
            label={label}
            options={selectOptions}
            combinedSelectedAndSearchOptions={selectedAndDropdownOptions}
            isLoading={isSelectLoading}
            values={selectedActorUrns}
            onUpdate={handleSelectChange}
            onSearchChange={handleSearch}
            showSearch={showSearch}
            filterResultsByQuery={false}
            isMultiSelect
            placeholder={placeholder}
            isDisabled={isDisabled}
            width={width}
            dataTestId={dataTestId}
            renderCustomSelectedValue={renderSelectedActorLabel}
            renderCustomOptionText={(option) => {
                const entity = dropdownActorEntities.find((e) => e.urn === option.value);
                return entity ? renderDropdownActorLabel(entity) : option.label;
            }}
        />
    );
};
