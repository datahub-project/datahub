import _, { debounce } from 'lodash';
import React, { useMemo, useState } from 'react';
import styled from 'styled-components';

import { Pill, SimpleSelect } from '@src/alchemy-components';
import { NestedSelectOption } from '@src/alchemy-components/components/Select/Nested/types';
import EntitySearchInputResultV2 from '@src/app/entityV2/shared/EntitySearchInput/EntitySearchInputResultV2';
import { useEntityRegistryV2 } from '@src/app/useEntityRegistry';
import { useGetAutoCompleteMultipleResultsLazyQuery } from '@src/graphql/search.generated';
import { Entity } from '@src/types.generated';

const Container = styled.div`
    width: 500px;
`;
interface EntitySelectorProps {
    defaultSuggestions: Entity[] | undefined;
    defaultSuggestionsLoading: boolean;
    onUpdate?: (selectedValues: string[]) => void;
    selected?: string[];
}

export const ProposalsEntitySelect = ({
    onUpdate,
    selected,
    defaultSuggestions,
    defaultSuggestionsLoading,
}: EntitySelectorProps) => {
    const entityRegistry = useEntityRegistryV2();

    const [selectedEntityList, setSelectedEntityList] = useState<any>(
        selected || defaultSuggestions?.map((e) => e?.urn) || [],
    );
    const [selectedEntityOptions, setSelectedEntityOptions] = useState<NestedSelectOption[]>([]);

    // Autocomplete results
    const [autoComplete, { data: autoCompleteData, loading: autoCompleteLoading }] =
        useGetAutoCompleteMultipleResultsLazyQuery();
    const autoCompleteSuggestions: Array<Entity> =
        autoCompleteData?.autoCompleteForMultiple?.suggestions.flatMap((suggestion) => suggestion.entities) || [];

    const [useSearch, setUseSearch] = useState(false);
    const suggestions = useSearch ? autoCompleteSuggestions : defaultSuggestions;

    // Prepare Options
    const availableEntityOptions = useMemo(() => {
        const options =
            suggestions?.map((entity) => ({
                value: entity.urn,
                label: entityRegistry.getDisplayName(entity.type, entity),
                entity,
            })) || [];
        const uniqueOptions = _.uniqBy(options, 'value');
        return uniqueOptions;
    }, [entityRegistry, suggestions]);

    const handleUpdate = (values: string[]) => {
        const newSelectedOptions: NestedSelectOption[] = [];
        values.forEach((value) => {
            const alreadySelected = newSelectedOptions.find((item) => item?.value === value);

            if (!alreadySelected) {
                const option =
                    availableEntityOptions.find((o) => o?.value === value) ||
                    selectedEntityOptions.find((o) => o?.value === value);

                if (option) {
                    newSelectedOptions.push(option as NestedSelectOption);
                }
            }
        });

        setSelectedEntityOptions(newSelectedOptions);
        setSelectedEntityList(newSelectedOptions.map((a) => a.value));
        onUpdate?.(newSelectedOptions.map((a) => a.value));
    };

    const removeLinkedAsset = (entityUrn) => {
        const selectedAssets = selectedEntityList?.filter((urn) => urn !== entityUrn);
        handleUpdate(selectedAssets);
    };

    const renderSelectedEntity = (selectedOption: NestedSelectOption) => {
        return selectedOption ? (
            <Pill
                key={selectedOption.value}
                label={selectedOption.label}
                rightIcon="Close"
                color="gray"
                variant="outline"
                onClickRightIcon={() => {
                    removeLinkedAsset(selectedOption.value);
                }}
                clickable
                size="sm"
            />
        ) : null;
    };

    const renderCustomEntityOption = (option: NestedSelectOption) => {
        return <>{option.entity && <EntitySearchInputResultV2 entity={option.entity} />}</>;
    };

    const handleSearch = (text: string) => {
        if (text) {
            autoComplete({
                variables: {
                    input: { query: text, limit: 10 },
                },
            });
            setUseSearch(true);
        } else {
            setUseSearch(false);
        }
    };

    return (
        <Container>
            <SimpleSelect
                options={defaultSuggestionsLoading ? [] : availableEntityOptions}
                isLoading={defaultSuggestionsLoading || autoCompleteLoading}
                size="sm"
                isMultiSelect
                placeholder="Select entity"
                width="full"
                optionListStyle={{
                    maxHeight: '30vh',
                    overflow: 'auto',
                }}
                selectedOptionListStyle={{
                    flexWrap: 'nowrap',
                    overflow: 'auto',
                    scrollbarWidth: 'none',
                    padding: '4px',
                }}
                onUpdate={handleUpdate}
                values={selectedEntityList}
                onSearchChange={debounce(handleSearch, 200)}
                showSearch
                showClear
                onClear={() => setUseSearch(false)}
                combinedSelectedAndSearchOptions={_.uniqBy(
                    [...availableEntityOptions, ...selectedEntityOptions],
                    'value',
                )}
                renderCustomSelectedValue={renderSelectedEntity}
                renderCustomOptionText={renderCustomEntityOption}
                selectLabelProps={{ variant: 'custom' }}
                optionListTestId="entities-options-list"
                data-testid="entities-select-input-type"
                ignoreMaxHeight
            />
        </Container>
    );
};
