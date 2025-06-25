import { Select, Text } from '@components';
import { debounce } from 'lodash';
import React, { useEffect, useMemo, useState } from 'react';
import styled from 'styled-components';

import { useGetRecommendations } from '@app/shared/recommendation';
import OwnerOption from '@app/sharedV2/owners/components/OwnerOption';
import OwnerPill from '@app/sharedV2/owners/components/OwnerPill';
import { OwnerSelectOption, PartialExtendedOwner } from '@app/sharedV2/owners/types';
import useDefaultOwner from '@app/sharedV2/owners/useDefaultOwner';
import useOwnershipTypes from '@app/sharedV2/owners/useOwnershipTypes';
import { isCorpUserOrCorpGroup, mapEntityToOwnerEntityType } from '@app/sharedV2/owners/utils';
import { MergeStrategy, mergeArrays } from '@app/utils/mergeArrays';

import { useGetAutoCompleteMultipleResultsLazyQuery } from '@graphql/search.generated';
import { Entity, EntityType, Owner } from '@types';

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
    onChange: (owners: Entity[]) => void;
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

    const defaultOwner = useDefaultOwner();

    const owners: PartialExtendedOwner[] = useMemo(
        () =>
            mergeArrays(!isEditForm && defaultOwner ? [defaultOwner] : [], existingOwners, (owner) => owner.owner.urn),
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
                        <OwnerPill owner={option.owner} onRemove={onRemove} readonly={!canEdit} />
                    )}
                    filterResultsByQuery={false}
                    renderCustomOptionText={(option) => <OwnerOption entity={option.owner.owner} />}
                />
            </FormSection>
        </SectionContainer>
    );
};

export default OwnersSection;
