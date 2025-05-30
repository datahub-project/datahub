import { Text } from '@components';
import { Select } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';

import { ExpandedOwner } from '@app/entityV2/shared/components/styled/ExpandedOwner/ExpandedOwner';
import { OwnerLabel } from '@app/shared/OwnerLabel';
import { useGetRecommendations } from '@app/shared/recommendation';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { useListOwnershipTypesQuery } from '@graphql/ownership.generated';
import { useGetAutoCompleteMultipleResultsLazyQuery } from '@graphql/search.generated';
import { Entity, EntityType, Owner, OwnerEntityType } from '@types';

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

const SelectInput = styled(Select)`
    width: 100%;

    .ant-select-selection-item {
        display: flex;
        align-items: center;
        border-radius: 16px;
        margin: 2px;
        height: 32px;
        padding-left: 4px;
        border: none;
    }

    .ant-select-selection-item-remove {
        margin-left: 8px;
        margin-right: 8px;
        color: rgba(0, 0, 0, 0.45);
    }
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
    onChange: (owners: PendingOwner[]) => void;
    sourceRefetch?: () => Promise<any>;
    isEditForm?: boolean;
}

/**
 * Component for owner selection and management
 */
const OwnersSection = ({
    selectedOwnerUrns,
    setSelectedOwnerUrns,
    existingOwners,
    onChange,
    sourceRefetch,
    isEditForm = false,
}: Props) => {
    const entityRegistry = useEntityRegistry();
    const [inputValue, setInputValue] = useState('');
    const [isSearching, setIsSearching] = useState(false);

    // Autocomplete query for owners across both CorpUser and CorpGroup types
    const [autoCompleteQuery, { data: autocompleteData, loading: searchLoading }] =
        useGetAutoCompleteMultipleResultsLazyQuery({
            fetchPolicy: 'no-cache',
        });

    const { recommendedData, loading: recommendationsLoading } = useGetRecommendations([
        EntityType.CorpGroup,
        EntityType.CorpUser,
    ]);

    // Lazy load ownership types
    const { data: ownershipTypesData } = useListOwnershipTypesQuery({
        variables: {
            input: {},
        },
    });

    const ownershipTypes = ownershipTypesData?.listOwnershipTypes?.ownershipTypes || [];
    const defaultOwnerType = ownershipTypes.length > 0 ? ownershipTypes[0].urn : undefined;

    // Get results from the recommendations or autocomplete
    const searchResults: Array<Entity> =
        !inputValue || inputValue.length === 0
            ? recommendedData
            : autocompleteData?.autoCompleteForMultiple?.suggestions?.flatMap((suggestion) => suggestion.entities) ||
              [];

    const finalResults = searchResults.filter(
        (res) => !existingOwners.map((owner) => owner.owner.urn).includes(res.urn),
    );

    // Debounced search handler
    const handleOwnerSearch = (text: string) => {
        setInputValue(text.trim());
        setIsSearching(true);

        if (text && text.trim().length > 0) {
            autoCompleteQuery({
                variables: {
                    input: {
                        types: [EntityType.CorpUser, EntityType.CorpGroup],
                        query: text.trim(),
                        limit: 10,
                    },
                },
            });
        }
    };

    // Renders a search result in the select dropdown
    const renderSearchResult = (entityItem: any) => {
        const avatarUrl =
            entityItem.type === EntityType.CorpUser ? entityItem.editableProperties?.pictureLink : undefined;
        const displayName = entityRegistry.getDisplayName(entityItem.type, entityItem);

        return (
            <Select.Option
                key={entityItem.urn}
                value={entityItem.urn}
                label={<OwnerLabel name={displayName} avatarUrl={avatarUrl} type={entityItem.type} />}
            >
                <OwnerLabel name={displayName} avatarUrl={avatarUrl} type={entityItem.type} />
            </Select.Option>
        );
    };

    // Handle select change - stores owners as pending until save
    const handleSelectChange = (newValues) => {
        setSelectedOwnerUrns(newValues);

        if (newValues.length > 0 && defaultOwnerType) {
            const newOwners = newValues.map((urn) => {
                const foundEntity = finalResults.find((e) => e.urn === urn);
                const ownerEntityType =
                    foundEntity && foundEntity.type === EntityType.CorpGroup
                        ? OwnerEntityType.CorpGroup
                        : OwnerEntityType.CorpUser;

                return {
                    ownerUrn: urn,
                    ownerEntityType,
                    ownershipTypeUrn: defaultOwnerType,
                };
            });

            onChange(newOwners);
        }
    };

    // Loading state for the select
    const isSelectLoading = recommendationsLoading || (isSearching && searchLoading);

    // Simplified conditional content for notFoundContent
    let notFoundContent: React.ReactNode = null;
    if (isSelectLoading) {
        notFoundContent = 'Loading...';
    } else if (inputValue && finalResults.length === 0) {
        notFoundContent = 'No results found';
    }

    return (
        <SectionContainer>
            <SectionHeader>
                <Text>Add Owners</Text>
            </SectionHeader>
            <FormSection>
                <SelectInput
                    mode="multiple"
                    placeholder="Search for users or groups"
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
