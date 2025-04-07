import React, { useState } from 'react';
import styled from 'styled-components';
import { Select } from 'antd';
import { Text } from '@components';
import { EntityType, OwnerEntityType } from '../../../types.generated';
import { useGetSearchResultsLazyQuery } from '../../../graphql/search.generated';
import { useEntityRegistry } from '../../useEntityRegistry';
import { useListOwnershipTypesQuery } from '../../../graphql/ownership.generated';
import { OwnerLabel } from '../../shared/OwnerLabel';
import { OwnersSectionProps } from './types';

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

    // Search queries for owners - fetchPolicy set to 'no-cache' to avoid stale data issues
    const [userSearch, { data: userSearchData, loading: userSearchLoading }] = useGetSearchResultsLazyQuery({
        fetchPolicy: 'no-cache',
    });
    const [groupSearch, { data: groupSearchData, loading: groupSearchLoading }] = useGetSearchResultsLazyQuery({
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

    // Combine search results
    const userSearchResults = userSearchData?.search?.searchResults?.map((result) => result.entity) || [];
    const groupSearchResults = groupSearchData?.search?.searchResults?.map((result) => result.entity) || [];
    const combinedSearchResults = [...userSearchResults, ...groupSearchResults];

    // Search handlers with debounce to reduce API calls
    const handleSearch = (type: EntityType, text: string, searchQuery: any) => {
        if (!text || text.trim().length < 2) return;

        searchQuery({
            variables: {
                input: {
                    type,
                    query: text,
                    start: 0,
                    count: 5,
                },
            },
        });
    };

    // Debounced search handler
    const handleOwnerSearch = (text: string) => {
        setInputValue(text.trim());
        setIsSearching(true);

        if (text && text.trim().length > 1) {
            handleSearch(EntityType.CorpUser, text.trim(), userSearch);
            handleSearch(EntityType.CorpGroup, text.trim(), groupSearch);
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
                const foundEntity = combinedSearchResults.find((e) => e.urn === urn);
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
    const isSelectLoading = isSearching && (userSearchLoading || groupSearchLoading);

    // Simplified conditional content for notFoundContent
    let notFoundContent: React.ReactNode = null;
    if (isSelectLoading) {
        notFoundContent = 'Loading...';
    } else if (inputValue && combinedSearchResults.length === 0) {
        notFoundContent = 'No results found';
    }

    return (
        <SectionContainer>
            <SectionHeader>
                <Text>Owners</Text>
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
                    {combinedSearchResults.map((entity) => renderSearchResult(entity))}
                </SelectInput>
            </FormSection>
        </SectionContainer>
    );
};

export default OwnersSection;
