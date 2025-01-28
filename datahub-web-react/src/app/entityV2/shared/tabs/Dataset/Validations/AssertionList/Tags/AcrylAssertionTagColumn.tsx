import React, { useState } from 'react';
import styled from 'styled-components';
import { message } from 'antd';
import Tag from '@src/app/sharedV2/tags/tag/Tag';
import { REDESIGN_COLORS } from '@src/app/entityV2/shared/constants';
import { Plus } from 'phosphor-react';
import { useGetRecommendations } from '@src/app/shared/recommendation';
import { handleBatchError } from '@src/app/entityV2/shared/utils';
import { useBatchAddTagsMutation, useBatchRemoveTagsMutation } from '@src/graphql/mutations.generated';
import { EntityType, Entity, TagAssociation, Tag as TagType } from '@src/types.generated';
import { getColor } from '@src/alchemy-components/theme/utils';
import { SelectItemPopover } from '@src/alchemy-components/components/SelectItemsPopover';
import DataHubTooltip from '@src/alchemy-components/components/Tooltip/Tooltip';

const StyledTagContainer = styled.div`
    display: flex;
    align-items: center;
    justify-content: center;
    flex-wrap: wrap;
    cursor: pointer;
    gap: 4px;
`;

const StyledPill = styled.div<{ color?: string; backgroundColor?: string }>`
    display: flex;
    gap: 4px;
    align-items: center;
    justify-content: center;
    background-color: ${(props) => props.backgroundColor || 'none'};
    height: 24px;
    width: 24px;
    color: ${(props) => props.color || '#5F6685'};
    border-radius: 100px;
    transition: all 0.2s;
    &:hover {
        color: black;
        background-color: ${getColor('gray', 100)};
    }
`;

const AdditionalPillCount = styled.div`
    display: flex;
    flex-wrap: wrap;
    gap: 2px;
    align-items: center;
    font-size: 12px;
    font-family: Mulish;
    color: ${REDESIGN_COLORS.BODY_TEXT};
`;

const TooltipTitleWrapper = styled.div`
    cursor: pointer;
    padding: 8px 4px 0px;
`;

const TooltipMoreText = styled.div`
    color: ${getColor('gray', 500)};
    font-size: 12px;
    margin-bottom: 4px;
`;

const MAX_TAGS_FOR_HOVER = 5;

interface AcrylAssertionTagColumnProps {
    record: any;
    refetch?: () => void;
}

export const AcrylAssertionTagColumn: React.FC<AcrylAssertionTagColumnProps> = ({ record, refetch }) => {
    const [popoverVisible, setPopoverVisible] = useState(false);

    const { recommendedData: allGlobalTags } = useGetRecommendations([EntityType.Tag]);
    // GraphQL mutations for adding/removing entities
    const [batchAddTagsMutation] = useBatchAddTagsMutation();
    const [batchRemoveTagsMutation] = useBatchRemoveTagsMutation();

    const totalTagsLength = record?.tags?.length || 0;
    // just take first tag if there are more than 1 tag
    const displayTags = totalTagsLength >= 2 ? [record.tags[0]] : record.tags;
    const remainingTagsCount = totalTagsLength - displayTags?.length || 0;

    const selectedTags = record?.tags?.map((global) => global.tag) || [];

    const tagsPreview = (
        <>
            {displayTags?.map((tag) => (
                <Tag
                    tag={tag}
                    options={{
                        shouldNotOpenDrawerOnClick: true,
                        shouldNotAddBottomMargin: true,
                        shouldShowEllipses: true,
                    }}
                    maxWidth={100}
                />
            ))}
            {remainingTagsCount > 0 ? (
                <DataHubTooltip
                    overlayInnerStyle={{ backgroundColor: 'white' }}
                    open={popoverVisible ? false : undefined}
                    title={
                        <TooltipTitleWrapper onClick={() => setPopoverVisible(true)}>
                            {record?.tags?.slice(1, MAX_TAGS_FOR_HOVER).map((tag) => (
                                <Tag
                                    tag={{ tag: tag.tag } as TagAssociation}
                                    options={{ shouldNotOpenDrawerOnClick: true }}
                                    maxWidth={120}
                                    tagStyle={{ marginBottom: 4 }}
                                />
                            ))}
                            {(record?.tags?.length ?? 0) > MAX_TAGS_FOR_HOVER ? (
                                <TooltipMoreText>+ {record.tags.length - MAX_TAGS_FOR_HOVER} more</TooltipMoreText>
                            ) : null}
                        </TooltipTitleWrapper>
                    }
                >
                    <AdditionalPillCount>
                        <Plus />
                        <span>{remainingTagsCount}</span>
                    </AdditionalPillCount>
                </DataHubTooltip>
            ) : (
                <StyledPill>
                    <Plus />
                </StyledPill>
            )}
        </>
    );

    const addTag = (
        <StyledPill>
            <Plus />
        </StyledPill>
    );

    const handleTagContainerClick = (e: React.MouseEvent) => {
        e.stopPropagation();
    };
    const handleClosePopover = () => {
        setPopoverVisible(false);
    };

    // Adds multiple entities to the resource
    const batchAddTags = async (newTags: string[]) => {
        try {
            await batchAddTagsMutation({
                variables: {
                    input: {
                        tagUrns: newTags,
                        resources: [{ resourceUrn: record.urn }],
                    },
                },
            });
        } catch (e) {
            message.error(handleBatchError(newTags, e, 'Failed to add entities.'));
        }
    };

    // Removes multiple entities from the resource
    const batchRemoveTags = async (removedTags: string[]) => {
        try {
            await batchRemoveTagsMutation({
                variables: {
                    input: {
                        tagUrns: removedTags,
                        resources: [{ resourceUrn: record.urn }],
                    },
                },
            });
        } catch (e) {
            message.error(handleBatchError(removedTags, e, 'Failed to remove entities.'));
        }
    };

    const handleSelectionChange = async ({ selectedItems, removedItems }) => {
        if (selectedItems?.length || removedItems?.length) {
            try {
                // Add new entities
                if (selectedItems?.length) {
                    await batchAddTags(selectedItems as string[]);
                }
                // Remove unselected entities
                if (removedItems?.length) {
                    await batchRemoveTags(removedItems as string[]);
                }

                // Notify success and refresh UI
                message.success('Tags Updated!', 2);
                setPopoverVisible(false);
                refetch?.();
            } catch (e) {
                console.log(e);
            }
        }
    };

    return (
        <SelectItemPopover
            key={`${popoverVisible}`}
            entities={(allGlobalTags as Entity[]) || []}
            selectedItems={selectedTags}
            refetch={refetch}
            onClose={handleClosePopover}
            entityType={EntityType.Tag}
            handleSelectionChange={handleSelectionChange}
            visible={popoverVisible}
            onVisibleChange={setPopoverVisible}
            renderOption={(option) => {
                return (
                    <Tag
                        tag={{ tag: option.item as TagType, associatedUrn: option.item?.urn }}
                        options={{ shouldNotOpenDrawerOnClick: true }}
                        maxWidth={120}
                    />
                );
            }}
        >
            <StyledTagContainer onClick={handleTagContainerClick}>
                {totalTagsLength > 0 ? tagsPreview : addTag}
            </StyledTagContainer>
        </SelectItemPopover>
    );
};
