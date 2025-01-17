import React from 'react';
import styled from 'styled-components/macro';
import { colors } from '@src/alchemy-components';
import { Tooltip } from 'antd';
import { BookmarkSimple, BookmarksSimple } from '@phosphor-icons/react';
import { ANTD_GRAY_V2, REDESIGN_COLORS } from '../entityV2/shared/constants';
import { EntityType, Maybe } from '../../types.generated';
import { generateColorFromPalette } from './colorUtils';
import { GenericEntityProperties } from '../entity/shared/types';

const SmallDescription = styled.div`
    color: ${REDESIGN_COLORS.SUB_TEXT};
    overflow: hidden;
`;

const EntityDetailsLeftColumn = styled.div`
    display: flex;
    gap: 15px;
    align-items: center;
`;

const EntityDetailsRightColumn = styled.div`
    margin-right: 5px;
    svg {
        display: none;
    }
`;

const BookmarkIconWrapper = styled.div<{ urnText: string }>`
    width: 40px;
    height: 40px;
    display: flex;
    justify-content: center;
    align-items: center;
    background-color: ${(props) => generateColorFromPalette(props.urnText)};
    border-radius: 11px;
    margin-right: 12px;
    position: relative;
    overflow: hidden;
    flex-shrink: 0; /* Prevents the icon from being squashed */
`;

const EntityDetails = styled.div`
    display: flex;
    justify-content: space-between;
    align-items: center;
    border-bottom: 1px solid ${REDESIGN_COLORS.LIGHT_GREY};
    padding: 20px 0 20px 0;
    margin: 0 23px 0 19px;
`;

const EntityDetailsWrapper = styled.div<{ type: EntityType }>`
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 16px 16px;
    border: 1px solid #ebecf0;
    margin: 0px 12px;
    border-radius: 16px;
    position: relative;
    overflow: hidden;

    &:hover > ${EntityDetails} > ${EntityDetailsLeftColumn} > ${BookmarkIconWrapper} > svg > g > path {
        transition: 0.15s;
        fill: rgba(216, 160, 75, 1);
    }

    &:hover > ${EntityDetails} > ${EntityDetailsRightColumn} > svg {
        transition: 0.15s;
        display: block;
    }

    &:hover {
        transition: 0.15s;
        background-color: ${colors.gray[100]};
    }
`;

const EntityName = styled.div`
    color: ${REDESIGN_COLORS.SUBTITLE};
    font-size: 14px;
    font-weight: 400;
    font-weight: bold;
`;

const EntityTitleWrapper = styled.div`
    display: flex;
    align-items: center;
    align-items: center;
`;

const NameAndDescription = styled.div`
    display: flex;
    flex-direction: column;
    overflow hidden;
`;

const BookmarkRibbon = styled.span<{ urnText: string }>`
    position: absolute;
    left: -11px;
    top: 7px;
    width: 40px;
    transform: rotate(-45deg);
    padding: 4px;
    opacity: 1;
    background-color: rgba(0, 0, 0, 0.2);
`;

const GlossaryItemCount = styled.span<{ count: number }>`
    display: flex;
    align-items: center;
    gap: 5px;
    border-radius: 20px;
    background: ${ANTD_GRAY_V2[14]};
    color: ${(props) => (props.count > 0 ? REDESIGN_COLORS.SUB_TEXT : colors.gray[400])};
    padding: 5px 10px;
    width: max-content;
    svg {
        height: 14px;
        width: 14px;
        path {
            fill: ${(props) => (props.count > 0 ? REDESIGN_COLORS.SUB_TEXT : colors.gray[400])};
        }
    }
    border: 1px solid transparent;
    :hover {
        border: 1px solid ${(props) => (props.count > 0 ? ANTD_GRAY_V2[13] : 'transparent')};
    }
`;

const CountText = styled.span`
    font-size: 10px;
    font-weight: 400;
`;

const Icons = styled.div`
    display: flex;
    flex-direction: row;
    align-items: center;
    gap: 5px;
`;

const MAX_DESCRIPTION_LENGTH = 100;
const MAX_DEPTH_QUERIED = 4;

interface Props {
    name: string;
    description: string | undefined;
    type: EntityType;
    urn: string;
    entityData: GenericEntityProperties | null;
    nodeCount?: Maybe<number>;
    termCount?: Maybe<number>;
    maxDepth?: number;
}

const GlossaryListCard = (props: Props) => {
    const { name, description, type, entityData } = props;
    const isDescriptionTruncated = description && description.length > MAX_DESCRIPTION_LENGTH;
    const truncatedDescription = description?.slice(0, MAX_DESCRIPTION_LENGTH);
    const isExceedingMaxDepth = (props.maxDepth || 0) > MAX_DEPTH_QUERIED;

    return (
        <EntityDetailsWrapper type={props.type}>
            {type === EntityType.GlossaryNode ? (
                <EntityTitleWrapper>
                    <BookmarkIconWrapper urnText={entityData?.urn || ''}>
                        <BookmarkRibbon urnText={entityData?.urn || ''} />
                        <BookmarksSimple color="white" size="16px" weight="bold" />
                    </BookmarkIconWrapper>
                    <NameAndDescription>
                        <EntityName>{name}</EntityName>
                        {description && (
                            <SmallDescription>
                                <>
                                    {truncatedDescription}
                                    {isDescriptionTruncated ? '...' : null}
                                </>
                            </SmallDescription>
                        )}
                    </NameAndDescription>
                </EntityTitleWrapper>
            ) : (
                <EntityTitleWrapper>
                    <BookmarkIconWrapper urnText={entityData?.urn || ''}>
                        <BookmarkRibbon urnText={entityData?.urn || ''} />
                        <BookmarkSimple color="white" size="16px" weight="bold" />
                    </BookmarkIconWrapper>
                    <NameAndDescription>
                        <EntityName>{name}</EntityName>
                        {description && (
                            <SmallDescription>
                                <>
                                    {truncatedDescription}
                                    {isDescriptionTruncated ? '...' : null}
                                </>
                            </SmallDescription>
                        )}
                    </NameAndDescription>
                </EntityTitleWrapper>
            )}
            {type === EntityType.GlossaryNode ? (
                <Icons>
                    <Tooltip
                        title={`Contains ${props.nodeCount} ${props.nodeCount === 1 ? 'term group' : 'term groups'}`}
                        placement="top"
                        showArrow={false}
                    >
                        <GlossaryItemCount count={props.nodeCount || 0}>
                            <BookmarksSimple />
                            <CountText>
                                {' '}
                                {props.nodeCount}
                                {isExceedingMaxDepth && `+`}
                            </CountText>
                        </GlossaryItemCount>
                    </Tooltip>
                    <Tooltip
                        title={`Contains ${props.termCount} ${props.termCount === 1 ? 'term' : 'terms'}`}
                        placement="top"
                        showArrow={false}
                    >
                        <GlossaryItemCount count={props.termCount || 0}>
                            <BookmarkSimple />
                            <CountText>
                                {' '}
                                {props.termCount}
                                {isExceedingMaxDepth && `+`}
                            </CountText>
                        </GlossaryItemCount>
                    </Tooltip>
                </Icons>
            ) : null}
        </EntityDetailsWrapper>
    );
};

export default GlossaryListCard;
