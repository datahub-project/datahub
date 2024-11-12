import React from 'react';
import { Button, Checkbox, Empty, Typography } from 'antd';
import styled from 'styled-components';
import { LoadingOutlined } from '@ant-design/icons';
import { GlobalTags, TagAssociation } from '@src/types.generated';
import { REDESIGN_COLORS } from '@src/app/entityV2/shared/constants';
import { AcrylListSearch } from '@src/app/entityV2/shared/components/ListSearch/AcrylListSearch';
import { useTagOperations } from './hooks'; // Import your custom hook
import { AcrylTagCheckboxGroup } from './AcrylTagCheckboxGroup';

interface AcrylAssertionSelectTagsProps {
    tags: GlobalTags[];
    selectedTags: TagAssociation[];
    resourceUrn: string;
    refetch?: () => void;
    onClose?: () => void;
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

const StyledSelectTagContainer = styled.div`
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

const StyledResetButton = styled(Button)`
    background-color: ${REDESIGN_COLORS.ICON_ON_DARK};
    border: 1px solid ${REDESIGN_COLORS.GREY_300};
    color: ${REDESIGN_COLORS.GREY_300};
    border-radius: 4px;
`;

const StyledUpdateButton = styled(Button)`
    background-color: ${REDESIGN_COLORS.TITLE_PURPLE};
    border: 1px solid ${REDESIGN_COLORS.TITLE_PURPLE};
    color: ${REDESIGN_COLORS.WHITE};
    border-radius: 4px;
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

export const AcrylAssertionSelectTags: React.FC<AcrylAssertionSelectTagsProps> = ({
    tags,
    selectedTags,
    resourceUrn,
    refetch,
    onClose,
}) => {
    const {
        filteredAddableOptions,
        filteredPreviouslyAddedOptions,
        selectedTagOptions,
        setSelectedTagOptions,
        handleUpdateTags,
        previouslyAddedOptions,
        searchText,
        handleSearchTags,
        tagsSearchResultsLoading,
        tagsSearchData,
    } = useTagOperations({ resourceUrn, selectedTags, refetch, tags, onClose });

    const handleCheckboxToggle = (urn: string) => {
        const options = selectedTagOptions ? [...selectedTagOptions] : [];
        if (options?.includes(urn)) {
            const index = options.indexOf(urn);
            options?.splice(index, 1);
        } else {
            options.push(urn);
        }
        setSelectedTagOptions(options);
    };

    const handleTagContainerClick = (e: React.MouseEvent) => {
        e.stopPropagation(); // Prevent the row click event from triggering
    };
    const isLoading = !tagsSearchData && tagsSearchResultsLoading;
    const hasAddableTagsMatchingFilters = filteredAddableOptions?.length > 0;
    const hasExistingTagsMatchingFilters = filteredPreviouslyAddedOptions?.length > 0;
    const hasExistingTags = previouslyAddedOptions?.length > 0;
    return (
        <StyledSelectTagContainer onClick={handleTagContainerClick}>
            <AcrylListSearch
                searchText={searchText}
                debouncedSetFilterText={handleSearchTags}
                matchResultCount={filteredPreviouslyAddedOptions?.length + filteredAddableOptions?.length}
                numRows={tagsSearchData?.autoComplete?.entities?.length || 0}
                entityTypeName="tag"
                options={{ hidePrefix: true, hideMatchCountText: true }}
            />
            {isLoading ? (
                <StyledLoader>
                    <LoadingOutlined />
                </StyledLoader>
            ) : (
                <StyledCheckBoxContainer>
                    <Checkbox.Group value={selectedTagOptions} style={{ width: '100%' }}>
                        {hasExistingTags ? (
                            <StyledGroupSection>
                                <StyledSubSection>Selected</StyledSubSection>
                                {hasExistingTagsMatchingFilters ? (
                                    <AcrylTagCheckboxGroup
                                        handleCheckboxToggle={handleCheckboxToggle}
                                        options={filteredPreviouslyAddedOptions}
                                    />
                                ) : null}

                                {!hasExistingTagsMatchingFilters && searchText ? (
                                    <StyledEmpty description="No tags found" />
                                ) : null}
                            </StyledGroupSection>
                        ) : null}
                        <StyledGroupSection>
                            {hasExistingTags ? (
                                <StyledSubSection>Add more</StyledSubSection>
                            ) : (
                                <div style={{ height: 8 }} />
                            )}
                            {hasAddableTagsMatchingFilters ? (
                                <AcrylTagCheckboxGroup
                                    handleCheckboxToggle={handleCheckboxToggle}
                                    options={filteredAddableOptions}
                                />
                            ) : null}
                            {!hasAddableTagsMatchingFilters && searchText ? (
                                <StyledEmpty description="No tags found" />
                            ) : null}
                        </StyledGroupSection>
                    </Checkbox.Group>
                </StyledCheckBoxContainer>
            )}
            <StyledFooter>
                {hasExistingTags ? (
                    <StyledResetButton onClick={() => handleUpdateTags({ isRemoveAll: true })}>
                        Remove All
                    </StyledResetButton>
                ) : null}
                <StyledUpdateButton
                    style={{ width: !hasExistingTags ? '100%' : '' }}
                    onClick={() => handleUpdateTags({})}
                >
                    Update
                </StyledUpdateButton>
            </StyledFooter>
        </StyledSelectTagContainer>
    );
};
