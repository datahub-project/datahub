import { LoadingOutlined } from '@ant-design/icons';
import { Checkbox, Empty, Typography } from 'antd';
import { CheckboxValueType } from 'antd/lib/checkbox/Group';
import React, { ReactNode } from 'react';
import styled from 'styled-components';

import { Button } from '@components/components/Button';
// Import your custom hook
import { SelectItemCheckboxGroup } from '@components/components/SelectItemsPopover/SelectItemCheckboxGroup';
import { useEntityOperations } from '@components/components/SelectItemsPopover/hooks';

import { InlineListSearch } from '@src/app/entityV2/shared/components/search/InlineListSearch';
import { REDESIGN_COLORS } from '@src/app/entityV2/shared/constants';
import { useEntityRegistry } from '@src/app/useEntityRegistry';
import { Entity, EntityType } from '@src/types.generated';

export interface SelectItemsProps {
    entities: Entity[];
    selectedItems: any[];
    refetch?: () => void;
    onClose?: () => void;
    entityType: EntityType;
    handleSelectionChange: ({
        selectedItems,
        removedItems,
    }: {
        selectedItems: CheckboxValueType[];
        removedItems: CheckboxValueType[];
    }) => void;
    renderOption?: (option: { value: string; label: ReactNode | string; item?: any }) => React.ReactNode;
}

const StyledSubSection = styled(Typography.Text)`
    margin-bottom: 12px;
    display: flex;
    justify-content: space-between;
    font-weight: 700;
    line-height: 15.06px;
    color: #5f6685;
`;

const StyledFooter = styled.div`
    display: flex;
    justify-content: center;
    gap: 16px;
    padding: 8px 0 0 0;
    border-top: 1px solid ${REDESIGN_COLORS.SILVER_GREY};
`;

const StyledSelectContainer = styled.div`
    display: flex;
    flex-direction: column;
    padding-bottom: 4px;
    padding-top: 8px;
`;

const StyledGroupSection = styled.div`
    &&& .ant-empty.ant-empty-normal {
        margin: 0 !important;
    }
`;

const StyledUpdateButton = styled(Button)`
    display: flex;
    justify-content: center;
`;

const StyledCheckBoxContainer = styled.div`
    max-height: 250px;
    min-height: 250px;
    overflow: auto;
    width: 100%;
    padding: 12px 0;
`;

const StyledLoader = styled.div`
    display: flex;
    justify-content: center;
    align-items: center;
    min-height: 250px;
    width: 100%;
`;

const StyledEmpty = styled(Empty)`
    .ant-empty-image {
        display: none;
    }
    color: #8d95b1;
    margin-bottom 12px;
`;

export const SelectItems: React.FC<SelectItemsProps> = ({
    entities,
    selectedItems,
    refetch,
    onClose,
    entityType,
    handleSelectionChange,
    renderOption,
}) => {
    const {
        filteredAddableOptions,
        filteredPreviouslyAddedOptions,
        selectedOptions,
        setSelectedOptions,
        handleUpdate,
        previouslyAddedOptions,
        searchText,
        handleSearchEntities,
        entitySearchResultsLoading,
        searchData,
    } = useEntityOperations({
        selectedItems,
        refetch,
        entities,
        onClose,
        entityType,
        handleSelectionChange,
    });

    const entityRegistry = useEntityRegistry();

    const handleCheckboxToggle = (urn: string) => {
        const options = selectedOptions ? [...selectedOptions] : [];
        if (options?.includes(urn)) {
            const index = options.indexOf(urn);
            options?.splice(index, 1);
        } else {
            options.push(urn);
        }
        setSelectedOptions(options);
    };

    const handleContainerClick = (e: React.MouseEvent) => {
        e.stopPropagation(); // Prevent the row click event from triggering
    };

    const isLoading = !searchData && entitySearchResultsLoading;
    const hasAddableEntitiesMatchingFilters = filteredAddableOptions?.length > 0;
    const hasExistingEntitiesMatchingFilters = filteredPreviouslyAddedOptions?.length > 0;
    const hasExistingEntities = previouslyAddedOptions?.length > 0;
    const entityName = entityRegistry.getCollectionName(entityType)?.toLowerCase();
    const emptyMessage = `No ${entityName} found`;
    return (
        <StyledSelectContainer onClick={handleContainerClick}>
            <InlineListSearch
                searchText={searchText}
                debouncedSetFilterText={handleSearchEntities}
                matchResultCount={filteredPreviouslyAddedOptions?.length + filteredAddableOptions?.length}
                numRows={searchData?.autoComplete?.entities?.length || 0}
                entityTypeName={entityType}
                options={{ hidePrefix: true, hideMatchCountText: true, placeholder: 'Search for tags...' }}
            />
            {isLoading ? (
                <StyledLoader>
                    <LoadingOutlined />
                </StyledLoader>
            ) : (
                <StyledCheckBoxContainer>
                    <Checkbox.Group value={selectedOptions} style={{ width: '100%' }}>
                        {hasExistingEntities ? (
                            <StyledGroupSection>
                                <StyledSubSection>Selected</StyledSubSection>
                                {hasExistingEntitiesMatchingFilters && (
                                    <SelectItemCheckboxGroup
                                        selectedOptions={selectedOptions}
                                        handleCheckboxToggle={handleCheckboxToggle}
                                        options={filteredPreviouslyAddedOptions}
                                        renderOption={renderOption}
                                    />
                                )}

                                {!hasExistingEntitiesMatchingFilters && searchText && (
                                    <StyledEmpty description={emptyMessage} />
                                )}
                            </StyledGroupSection>
                        ) : null}
                        <StyledGroupSection>
                            {(hasExistingEntitiesMatchingFilters || searchText) && (
                                <StyledSubSection>Add more</StyledSubSection>
                            )}
                            {hasAddableEntitiesMatchingFilters && (
                                <SelectItemCheckboxGroup
                                    selectedOptions={selectedOptions}
                                    handleCheckboxToggle={handleCheckboxToggle}
                                    options={filteredAddableOptions}
                                    renderOption={renderOption}
                                />
                            )}
                            {!hasAddableEntitiesMatchingFilters && searchText && (
                                <StyledEmpty description={emptyMessage} />
                            )}
                        </StyledGroupSection>
                    </Checkbox.Group>
                </StyledCheckBoxContainer>
            )}
            <StyledFooter>
                {hasExistingEntities && (
                    <Button variant="outline" color="gray" onClick={() => handleUpdate({ isRemoveAll: true })}>
                        Remove All
                    </Button>
                )}
                <StyledUpdateButton
                    style={{ width: !hasExistingEntities ? '100%' : '' }}
                    onClick={() => handleUpdate({})}
                >
                    Update
                </StyledUpdateButton>
            </StyledFooter>
        </StyledSelectContainer>
    );
};
