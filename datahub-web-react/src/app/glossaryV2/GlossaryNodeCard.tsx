import React from 'react';
import styled from 'styled-components/macro';
import { Tooltip, Typography } from 'antd';
import { Maybe } from 'graphql/jsutils/Maybe';
import { colors } from '@src/alchemy-components';
import { BookmarkSimple, BookmarksSimple } from '@phosphor-icons/react';
import { DisplayProperties } from '../../types.generated';
import { ANTD_GRAY, ANTD_GRAY_V2, REDESIGN_COLORS } from '../entityV2/shared/constants';
import { generateColorFromPalette } from './colorUtils';

interface GlossaryItemCardHeaderProps {
    color: string;
}

// there may be a good constant for this but I couldn't find one --Gabe
// feel free to replace this color at a later date
const DISABLED_TEXT_COLOR = '#c5c6c9';

const GlossaryItemCardHeader = styled.div<GlossaryItemCardHeaderProps>`
    display: flex;
    padding: 20px 20px 20px 30px;
    justify-content: start;
    border-radius: 12px 12px 0px 0px;
    position: relative;
    overflow: hidden;
    gap: 5px;
    background-color: ${(props) => props.color};

    svg {
        height: 22px;
        width: 22px;
        path {
            fill: white;
        }
    }
`;

const GlossaryItemCardWrapper = styled.div`
    padding: 12px;
    border-radius: 13px;

    &:hover {
        transition: 0.15s;
        background-color: ${colors.gray[100]};
    }
`;

const GlossaryItemCard = styled.div`
    display: flex;
    flex-direction: column;
    border-radius: 13px;
    border: 1px solid ${REDESIGN_COLORS.LIGHT_GREY_BORDER};
    background: ${ANTD_GRAY[1]};
    transition: 0.15s;
    height: 100%;
    width: 100%;

    &:hover {
        transition: 0.15s;
        border-color: ${REDESIGN_COLORS.TITLE_PURPLE};
    }

    &:hover > ${GlossaryItemCardHeader} {
        transition: 0.15s;
        opacity: 0.9 !important;
    }
`;

const GlossaryItemCount = styled.span<{ count: number }>`
    display: flex;
    align-items: center;
    gap: 5px;
    border-radius: 20px;
    background: ${(props) => (props.count > 0 ? ANTD_GRAY_V2[14] : ANTD_GRAY_V2[14])};
    color: ${(props) => (props.count > 0 ? REDESIGN_COLORS.SUB_TEXT : DISABLED_TEXT_COLOR)};
    padding: 5px 10px;
    width: max-content;
    svg {
        height: 14px;
        width: 14px;
        path {
            fill: ${(props) => (props.count > 0 ? REDESIGN_COLORS.SUB_TEXT : DISABLED_TEXT_COLOR)};
        }
    }
    border: 1px solid transparent;
    :hover {
        border: 1px solid ${(props) => (props.count > 0 ? ANTD_GRAY_V2[13] : 'transparent')};
    }
`;

const GlossaryItemBadge = styled.span`
    position: absolute;
    left: -35px;
    top: 8px;
    width: 100px;
    transform: rotate(-45deg);
    padding: 8px;
    opacity: 1;
    background-color: rgba(0, 0, 0, 0.17);
`;

const GlossaryItemCardDetails = styled.div`
    display: flex;
    flex-direction: column;
    padding: 13px 16px;
    gap: 10px;
`;

const GlossaryCardHeader = styled(Typography)`
    color: #f9fafc;
    font-size: 16px;
    font-weight: 500;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
`;

const GlossaryItemCardDescription = styled(Typography)`
    color: ${REDESIGN_COLORS.SUB_TEXT};
    font-size: 12px;
    font-weight: 400;
    margin-top: 1px;
    width: 100%;
    min-height: 30px;
    overflow: hidden;
    display: -webkit-box;
    -webkit-line-clamp: 2;
    -webkit-box-orient: vertical;
`;

const CountText = styled.span`
    font-size: 10px;
    font-weight: 400;
`;

interface Props {
    name: string;
    description: string | undefined;
    termCount: number;
    nodeCount: number;
    displayProperties?: Maybe<DisplayProperties>;
    urn: string;
    maxDepth?: number;
}

const Icons = styled.div`
    display: flex;
    flex-direction: row;
    align-items: center;
    gap: 5px;
`;

const MAX_DEPTH_QUERIED = 4;

const GlossaryNodeCard = (props: Props) => {
    const { name, description, termCount, nodeCount, displayProperties, urn, maxDepth } = props;
    const glossaryColor = displayProperties?.colorHex || generateColorFromPalette(urn);

    const isExceedingMaxDepth = (maxDepth || 0) > MAX_DEPTH_QUERIED;

    return (
        <GlossaryItemCardWrapper>
            <GlossaryItemCard>
                <GlossaryItemCardHeader color={glossaryColor}>
                    <BookmarksSimple style={{ flexShrink: 0 }} />
                    <GlossaryCardHeader>{name}</GlossaryCardHeader>
                    <GlossaryItemBadge />
                </GlossaryItemCardHeader>
                <GlossaryItemCardDetails>
                    <GlossaryItemCardDescription>{description || '--'}</GlossaryItemCardDescription>
                    <Icons>
                        <Tooltip
                            title={`Contains ${nodeCount} ${props.nodeCount === 1 ? 'term group' : 'term groups'}`}
                            placement="top"
                            showArrow={false}
                        >
                            <GlossaryItemCount count={nodeCount}>
                                <BookmarksSimple />
                                <CountText>
                                    {' '}
                                    {nodeCount}
                                    {isExceedingMaxDepth && `+`}
                                </CountText>
                            </GlossaryItemCount>
                        </Tooltip>
                        <Tooltip
                            title={`Contains ${termCount} ${props.termCount === 1 ? 'term' : 'terms'}`}
                            placement="top"
                            showArrow={false}
                        >
                            <GlossaryItemCount count={termCount}>
                                <BookmarkSimple />
                                <CountText>
                                    {' '}
                                    {termCount}
                                    {isExceedingMaxDepth && `+`}
                                </CountText>
                            </GlossaryItemCount>
                        </Tooltip>
                    </Icons>
                </GlossaryItemCardDetails>
            </GlossaryItemCard>
        </GlossaryItemCardWrapper>
    );
};

export default GlossaryNodeCard;
