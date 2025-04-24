import { Text } from '@components';
import { Select } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';

import { OwnerLabel } from '@app/shared/OwnerLabel';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { useListOwnershipTypesQuery } from '@graphql/ownership.generated';
import { useGetSearchResultsForMultipleLazyQuery } from '@graphql/search.generated';
import { EntityType, OwnerEntityType } from '@types';

// Interface for pending owner
export interface PendingOwner {
    ownerUrn: string;
    ownerEntityType: OwnerEntityType;
    ownershipTypeUrn: string;
}

// Owners section props
export interface OwnersSectionProps {
    selectedOwnerUrns: string[];
    setSelectedOwnerUrns: React.Dispatch<React.SetStateAction<string[]>>;
    pendingOwners: PendingOwner[];
    setPendingOwners: React.Dispatch<React.SetStateAction<PendingOwner[]>>;
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

/**
 * Component for owner selection and management
 */
const OwnersSection: React.FC<OwnersSectionProps> = ({
    selectedOwnerUrns,
    setSelectedOwnerUrns,
    pendingOwners,
    setPendingOwners,
}) => {
    const entityRegistry = useEntityRegistry();
    const [inputValue, setInputValue] = useState('');
    const [isSearching, setIsSearching] = useState(false);

    // Search query for owners across both CorpUser and CorpGroup types
    const [searchAcrossEntities, { data: searchData, loading: searchLoading }] =
        useGetSearchResultsForMultipleLazyQuery({
            fetchPolicy: 'no-cache',
        });

    // Lazy load ownership types
    const { data: ownershipTypesData } = useListOwnershipTypesQuery({
        variables: {
            input: {},
        },
    });

    const ownershipTypes = ownershipTypesData?.listOwnershipTypes?.ownershipTypes || [];
    const defaultOwnerType = ownershipTypes.length > 0 ? ownershipTypes[0].urn : undefined;

    // Get search results from the combined query
    const searchResults = searchData?.searchAcrossEntities?.searchResults?.map((result) => result.entity) || [];

    // Debounced search handler
    const handleOwnerSearch = (text: string) => {
        setInputValue(text.trim());
        setIsSearching(true);

        if (text && text.trim().length > 1) {
            searchAcrossEntities({
                variables: {
                    input: {
                        types: [EntityType.CorpUser, EntityType.CorpGroup],
                        query: text.trim(),
                        start: 0,
                        count: 10,
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
    const handleSelectChange = (values: any) => {
        const newValues = values as string[];
        setSelectedOwnerUrns(newValues);

        // Find new owner URNs that weren't previously selected
        const newOwnerUrns = newValues.filter((urn) => !pendingOwners.some((owner) => owner.ownerUrn === urn));

        if (newOwnerUrns.length > 0 && defaultOwnerType) {
            const newPendingOwners = newOwnerUrns.map((urn) => {
                const foundEntity = searchResults.find((e) => e.urn === urn);
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

            setPendingOwners([...pendingOwners, ...newPendingOwners]);
        }

        // Handle removed owners
        if (newValues.length < selectedOwnerUrns.length) {
            const removedUrns = selectedOwnerUrns.filter((urn) => !newValues.includes(urn));
            const updatedPendingOwners = pendingOwners.filter((owner) => !removedUrns.includes(owner.ownerUrn));
            setPendingOwners(updatedPendingOwners);
        }
    };

    // Loading state for the select
    const isSelectLoading = isSearching && searchLoading;

    // Simplified conditional content for notFoundContent
    let notFoundContent: React.ReactNode = null;
    if (isSelectLoading) {
        notFoundContent = 'Loading...';
    } else if (inputValue && searchResults.length === 0) {
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
                    {searchResults.map((entity) => renderSearchResult(entity))}
                </SelectInput>
            </FormSection>
        </SectionContainer>
    );
};

export default OwnersSection;
