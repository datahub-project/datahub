import { Select, Text } from '@components';
import { debounce } from 'lodash';
import React, { useEffect, useMemo, useState } from 'react';
import styled from 'styled-components';

import { useUserContext } from '@app/context/useUserContext';
import AutoCompleteEntityItem from '@app/searchV2/autoCompleteV2/AutoCompleteEntityItem';
import { useGetRecommendations } from '@app/shared/recommendation';
import OwnerPill from '@app/sharedV2/owners/components/OwnerPill';
import useOwnershipTypes from '@app/sharedV2/owners/hooks/useOwnershipTypes';
import { OwnerSelectOption, PartialExtendedOwner } from '@app/sharedV2/owners/types';
import { isCorpUserOrCorpGroup, mapEntityToOwnerEntityType } from '@app/sharedV2/owners/utils';
import { MergeStrategy, mergeArrays } from '@app/utils/mergeArrays';

import { useGetAutoCompleteMultipleResultsLazyQuery } from '@graphql/search.generated';
import { Entity, EntityType, Owner, OwnerType } from '@types';

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

// Owners section props
interface Props {
    selectedOwnerUrns: string[];
    setSelectedOwnerUrns: React.Dispatch<React.SetStateAction<string[]>>;
    existingOwners: Owner[];
    onChange: (owners: OwnerType[]) => void;
    isEditForm?: boolean;
    canEdit?: boolean;
}

/**
 * Component for owner selection and management
 */
const OwnersSection = ({
    selectedOwnerUrns,
    setSelectedOwnerUrns,
    existingOwners,
    onChange,
    isEditForm = false,
    canEdit = true,
}: Props) => {
    const [isInitialized, setIsInitialized] = useState<boolean>(false);
    const [inputValue, setInputValue] = useState('');

    const { user: defaultOwner } = useUserContext();

    const owners: PartialExtendedOwner[] = useMemo(
        () =>
            mergeArrays<PartialExtendedOwner>(
                !isEditForm && defaultOwner ? [{ owner: defaultOwner }] : [],
                existingOwners,
                (owner) => owner.owner.urn,
            ),
        [existingOwners, defaultOwner, isEditForm],
    );

    const [selectedOptions, setSelectedOptions] = useState<OwnerSelectOption[]>([]);

    const initialValues = useMemo(() => owners.map((owner) => owner.owner.urn), [owners]);

    // Initialization of state
    useEffect(() => {
        if (!isInitialized) {
            setSelectedOptions(owners.map((owner) => ({ value: owner.owner.urn, label: owner.owner.urn, owner })));
            setSelectedOwnerUrns(owners.map((owner) => owner.owner.urn));
            setIsInitialized(true);
        }
    }, [owners, isInitialized, setSelectedOwnerUrns]);

    // Autocomplete query for owners across both CorpUser and CorpGroup types
    const [autoCompleteQuery, { data: autocompleteData }] = useGetAutoCompleteMultipleResultsLazyQuery({
        fetchPolicy: 'no-cache',
    });

    const { recommendedData } = useGetRecommendations([EntityType.CorpGroup, EntityType.CorpUser]);

    const { defaultOwnershipType } = useOwnershipTypes();

    // Get results from the recommendations or autocomplete
    const searchResults: Entity[] = useMemo(() => {
        if (!inputValue || inputValue.length === 0) return recommendedData;

        return (
            autocompleteData?.autoCompleteForMultiple?.suggestions?.flatMap((suggestion) => suggestion.entities) ?? []
        );
    }, [inputValue, recommendedData, autocompleteData]);

    const options: OwnerSelectOption[] = useMemo(() => {
        return [
            ...selectedOptions,
            ...searchResults
                .filter(isCorpUserOrCorpGroup)
                .map((userOrGroup) => ({
                    value: userOrGroup.urn,
                    label: userOrGroup.urn,
                    owner: {
                        owner: userOrGroup,
                        ownerUrn: userOrGroup.urn,
                        ownerEntityType: mapEntityToOwnerEntityType(userOrGroup),
                        ownershipTypeUrn: defaultOwnershipType,
                    },
                }))
                .filter((option) => !selectedOptions.some((selectedOption) => selectedOption.value === option.value)),
        ];
    }, [searchResults, selectedOptions, defaultOwnershipType]);

    const handleOwnerSearch = debounce((text: string) => {
        const query = text.trim();
        setInputValue(query);
        autoCompleteQuery({
            variables: {
                input: {
                    types: [EntityType.CorpUser, EntityType.CorpGroup],
                    query,
                    limit: 10,
                },
            },
        });
    }, 300);

    const handleSelectChange = (newValues: string[]) => {
        setSelectedOwnerUrns(newValues);
        setSelectedOptions((prev) => {
            const newOptions: OwnerSelectOption[] = newValues
                .map((value) => options.find((option) => option.value === value))
                .filter((option): option is OwnerSelectOption => !!option);
            return mergeArrays<OwnerSelectOption>(
                prev.filter((option) => newValues.includes(option.value)),
                newOptions,
                (option) => option.value,
                MergeStrategy.preferAItems,
            );
        });
        onChange(options.filter((option) => newValues.includes(option.value)).map((option) => option.owner.owner));
    };

    return (
        <SectionContainer>
            <SectionHeader>
                <Text>Owners</Text>
            </SectionHeader>
            <FormSection>
                <Select
                    isMultiSelect
                    width="full"
                    values={selectedOwnerUrns}
                    initialValues={initialValues}
                    onUpdate={handleSelectChange}
                    options={options}
                    showSearch
                    onSearchChange={handleOwnerSearch}
                    selectLabelProps={{ variant: 'custom' }}
                    hideSelectedOptions
                    renderCustomSelectedValue={(option, onRemove) => (
                        <OwnerPill owner={option.owner.owner} onRemove={onRemove} readonly={!canEdit} hideLink />
                    )}
                    filterResultsByQuery={false}
                    renderCustomOptionText={(option) => (
                        <AutoCompleteEntityItem
                            entity={option.owner.owner}
                            customDetailsRenderer={() => null}
                            padding="0px"
                        />
                    )}
                    selectMinHeight="42px"
                    autoUpdate
                />
            </FormSection>
        </SectionContainer>
    );
};

export default OwnersSection;
