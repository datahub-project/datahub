import { Text } from '@components';
import React, { useEffect, useState } from 'react';
import styled from 'styled-components';

import { ExpandedOwner } from '@app/entityV2/shared/components/styled/ExpandedOwner/ExpandedOwner';
import { useGetRecommendations } from '@app/shared/recommendation';
import { SimpleSelect } from '@src/alchemy-components/components/Select/SimpleSelect';
import { SelectOption } from '@src/alchemy-components/components/Select/types';
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
    shouldSetOwnerEntities?: boolean;
}

/**
 * Component for owner selection and management using standard components
 */
const OwnersSection = ({
    selectedOwnerUrns,
    setSelectedOwnerUrns,
    existingOwners,
    onChange,
    placeholderOwners,
    sourceRefetch,
    isEditForm = false,
    shouldSetOwnerEntities = false,
}: Props<T>) => {
    const entityRegistry = useEntityRegistryV2();
    const [selectedOwnerEntities, setSelectedOwnerEntities] = useState<Entity[]>([]);

    // Auto-select placeholder owners if they're not already selected (only once on mount)
    useEffect(() => {
        if (placeholderOwners && placeholderOwners.length > 0 && selectedOwnerUrns.length === 0) {
            const placeholderUrns = placeholderOwners.map((owner) => owner.urn);
            setSelectedOwnerUrns(placeholderUrns);
        }
    }, [placeholderOwners, selectedOwnerUrns.length, setSelectedOwnerUrns]);

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
    const searchResults: Array<Entity> =
        autocompleteData?.autoCompleteForMultiple?.suggestions?.flatMap((suggestion) => suggestion.entities) ||
        recommendedData ||
        [];

    // Combine search results with placeholder owners
    const allResults = [...(placeholderOwners || []), ...searchResults];

    const finalResults = allResults.filter((res) => !existingOwners.map((owner) => owner.owner.urn).includes(res.urn));

    // Convert entities to SelectOption format
    const selectOptions: SelectOption[] = finalResults.map((entity) => ({
        value: entity.urn,
        label: entityRegistry.getDisplayName(entity.type, entity),
        description: entity.type === EntityType.CorpUser ? (entity as any)?.properties?.email : undefined,
    }));

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
                return finalResults.find((e) => e.urn === urn) || selectedOwnerEntities.find((e) => e.urn === urn);
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
                    filterOption={false}
                    onSearch={handleOwnerSearch}
                    onChange={handleSelectChange}
                    value={selectedOwnerUrns}
                    loading={isSelectLoading}
                    notFoundContent={notFoundContent}
                    optionLabelProp="label"
                >
                    {finalResults.map((entity) => renderSearchResult(entity))}
                </SelectInput>
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
