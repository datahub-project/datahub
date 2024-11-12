import React, { useState } from 'react';
import styled from 'styled-components';
import { Popover, Tooltip } from 'antd';
import Tag from '@src/app/sharedV2/tags/tag/Tag';
import { REDESIGN_COLORS } from '@src/app/entityV2/shared/constants';
import { Plus } from 'phosphor-react';
import { useGetRecommendations } from '@src/app/shared/recommendation';
import { EntityType, GlobalTags } from '@src/types.generated';
import { getColor } from '@src/alchemy-components/theme/utils';
import { useEntityRegistry } from '@src/app/useEntityRegistry';
import { AcrylAssertionSelectTags } from './AcrylAssertionSelectTags';

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

const StyledPopover = styled(Popover)`
    .ant-popover-inner-content {
        padding-right: 0 !important; /* Remove right padding */
    }
    display: flex;
    justify-content: flex-start;
`;

interface AcrylAssertionTagColumnProps {
    record: any;
    refetch?: () => void;
}

export const AcrylAssertionTagColumn: React.FC<AcrylAssertionTagColumnProps> = ({ record, refetch }) => {
    const entityRegistry = useEntityRegistry();
    const [popoverVisible, setPopoverVisible] = useState(false);

    const { recommendedData: allGlobalTags } = useGetRecommendations([EntityType.Tag]);
    const totalTagsLength = record?.tags?.length || 0;
    // just take first tag if there are more than 1 tag
    const displayTags = totalTagsLength >= 2 ? [record.tags[0]] : record.tags;
    const remainingTagsCount = totalTagsLength - displayTags?.length || 0;
    const secondTag = record?.tags?.[1];
    const maybeSecondTagName = secondTag?.tag && entityRegistry.getDisplayName(EntityType.Tag, secondTag.tag);

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
                <Tooltip
                    title={
                        remainingTagsCount === 1 ? (
                            <b>{maybeSecondTagName}</b>
                        ) : (
                            <span>
                                <b>{maybeSecondTagName}</b> and {remainingTagsCount - 1} more.
                            </span>
                        )
                    }
                >
                    <AdditionalPillCount>
                        <Plus />
                        <span>{remainingTagsCount}</span>
                    </AdditionalPillCount>
                </Tooltip>
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

    return (
        <StyledPopover
            trigger="click"
            open={popoverVisible}
            onOpenChange={setPopoverVisible}
            content={
                <AcrylAssertionSelectTags
                    key={`${popoverVisible}`}
                    tags={allGlobalTags as GlobalTags[]}
                    selectedTags={record.tags}
                    resourceUrn={record.urn}
                    refetch={refetch}
                    onClose={handleClosePopover}
                />
            }
            showArrow={false}
        >
            <StyledTagContainer onClick={handleTagContainerClick}>
                {totalTagsLength > 0 ? tagsPreview : addTag}
            </StyledTagContainer>
        </StyledPopover>
    );
};
