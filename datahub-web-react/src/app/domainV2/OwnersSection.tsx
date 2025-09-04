import { Text } from '@components';
import React, { useEffect, useMemo, useRef, useState } from 'react';
import styled from 'styled-components';

import { ExpandedOwner } from '@app/entityV2/shared/components/styled/ExpandedOwner/ExpandedOwner';
import { deduplicateEntities, entitiesToSelectOptions } from '@app/entityV2/shared/utils/selectorUtils';
import { useGetRecommendations } from '@app/shared/recommendation';
import { SimpleSelect } from '@src/alchemy-components/components/Select/SimpleSelect';
import EntityIcon from '@src/app/searchV2/autoCompleteV2/components/icon/EntityIcon';
import { useEntityRegistryV2 } from '@src/app/useEntityRegistry';

import { useGetAutoCompleteMultipleResultsLazyQuery } from '@graphql/search.generated';
import { CorpUser, Entity, EntityType, Owner, OwnerEntityType } from '@types';

// Interface for pending owner
export interface PendingOwner {
    ownerUrn: string;
    ownerEntityType: OwnerEntityType;
    ownershipTypeUrn: string;
}

const SectionContainer = styled.div`
    margin-bottom: 24px;
`;

const SectionHeader = styled.div`
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 8px;
`;

const FormSection = styled.div`
    margin-bottom: 16px;
`;

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

const OwnersContainer = styled.div`
    display: flex;
    flex-wrap: wrap;
    gap: 8px;
    margin-top: 8px;
`;

// Owners section props
interface Props {
    selectedOwnerUrns: string[];
    setSelectedOwnerUrns: React.Dispatch<React.SetStateAction<string[]>>;
    existingOwners: Owner[];
    onChange: (owners: Entity[]) => void;
    placeholderOwners?: (Entity | CorpUser)[];
    sourceRefetch?: () => Promise<any>;
    isEditForm?: boolean;
    canEdit?: boolean;
}

/**
 * Component for owner selection and management using standard components
 * The goal is to replace sharedV2/owners/OwnersSection.tsx with this component.
 */
const OwnersSection = ({
    selectedOwnerUrns,
    setSelectedOwnerUrns,
    existingOwners,
    onChange,
    placeholderOwners,
    sourceRefetch,
    isEditForm = false,
    canEdit = true,
}: Props) => {
    const entityRegistry = useEntityRegistryV2();
    const [selectedOwnerEntities, setSelectedOwnerEntities] = useState<Entity[]>([]);
    const [initialized, setInitialized] = useState(false);
    const hasAutoSelectedRef = useRef(false);

    // Auto-select placeholder owners ONLY ONCE on initial render when no owners are selected
    useEffect(() => {
        if (
            placeholderOwners &&
            placeholderOwners.length > 0 &&
            !hasAutoSelectedRef.current &&
            selectedOwnerUrns.length === 0
        ) {
            const placeholderUrns = placeholderOwners.map((owner) => owner.urn);
            setSelectedOwnerUrns(placeholderUrns);
            hasAutoSelectedRef.current = true;
        }

        // Mark as initialized when we have placeholders
        if (placeholderOwners && placeholderOwners.length > 0 && !initialized) {
            setInitialized(true);
        }
    }, [placeholderOwners, selectedOwnerUrns.length, setSelectedOwnerUrns, initialized]);

    // Autocomplete query for owners across both CorpUser and CorpGroup types
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

    // Sync selectedOwnerEntities with selectedOwnerUrns from parent
    useEffect(() => {
        const currentSelectedUrns = selectedOwnerEntities
            .map((e) => e.urn)
            .sort()
            .join(',');
        const newSelectedUrns = selectedOwnerUrns.sort().join(',');

        // Only update if the URNs have actually changed
        if (currentSelectedUrns !== newSelectedUrns) {
            const entities = selectedOwnerUrns
                .map((urn) => {
                    // Try to find in all available sources
                    return (
                        (placeholderOwners || []).find((e) => e.urn === urn) ||
                        searchResults.find((e) => e.urn === urn) ||
                        selectedOwnerEntities.find((e) => e.urn === urn)
                    );
                })
                .filter(Boolean) as Entity[];

            setSelectedOwnerEntities(entities);
        }
    }, [selectedOwnerUrns, placeholderOwners, searchResults, selectedOwnerEntities]);

    // Use utility to deduplicate entities from all sources
    const allOptionsEntities = deduplicateEntities({
        placeholderEntities: placeholderOwners,
        searchResults,
        selectedEntities: selectedOwnerEntities,
        existingEntityUrns: existingOwners.map((owner) => owner.owner.urn),
    });

    // Convert entities to SelectOption format using utility
    const selectOptions = entitiesToSelectOptions(allOptionsEntities, entityRegistry);

    // Handle search
    const handleSearch = (query: string) => {
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
    };

    // Render owner entity (similar to OwnerFilter)
    const renderOwnerEntity = (entity: Entity) => {
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
    };

    // Handle select change
    const handleSelectChange = (newValues: string[]) => {
        setSelectedOwnerUrns(newValues);

        const newEntities = newValues
            .map((urn) => {
                return (
                    allOptionsEntities.find((e) => e.urn === urn) || selectedOwnerEntities.find((e) => e.urn === urn)
                );
            })
            .filter(Boolean) as Entity[];

        setSelectedOwnerEntities(newEntities);

        if (newValues.length > 0) {
            onChange(newEntities);
        } else {
            onChange([]);
        }
    };

    // Loading state for the select
    const isSelectLoading = recommendationsLoading || searchLoading;

    return (
        <SectionContainer>
            <SectionHeader>
                <Text>Add Owners</Text>
            </SectionHeader>
            <FormSection>
                <SimpleSelect
                    options={selectOptions}
                    isLoading={isSelectLoading}
                    values={selectedOwnerUrns}
                    onUpdate={handleSelectChange}
                    onSearchChange={handleSearch}
                    showSearch
                    isMultiSelect
                    placeholder="Search for users or groups"
                    isDisabled={!canEdit}
                    width="full"
                    renderCustomOptionText={(option) => {
                        const entity = allOptionsEntities.find((e) => e.urn === option.value);
                        return entity ? renderOwnerEntity(entity) : option.label;
                    }}
                />
            </FormSection>
            {isEditForm && (
                <OwnersContainer>
                    {existingOwners.length > 0 ? (
                        existingOwners.map((owner) => (
                            <ExpandedOwner
                                key={owner.owner.urn}
                                entityUrn={owner.associatedUrn}
                                owner={owner}
                                refetch={sourceRefetch}
                                readOnly={!canEdit}
                            />
                        ))
                    ) : (
                        <div>No owners assigned</div>
                    )}
                </OwnersContainer>
            )}
        </SectionContainer>
    );
};

export default OwnersSection;
