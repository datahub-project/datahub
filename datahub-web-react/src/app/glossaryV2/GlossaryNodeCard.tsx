import { BookmarkSimple, BookmarksSimple } from '@phosphor-icons/react';
import { Tooltip, Typography } from 'antd';
import { Maybe } from 'graphql/jsutils/Maybe';
import React from 'react';
import styled from 'styled-components/macro';

import { useGenerateGlossaryColorFromPalette } from '@app/glossaryV2/colorUtils';

import { DisplayProperties } from '@types';

interface GlossaryItemCardHeaderProps {
    color: string;
}

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
            fill: ${(props) => props.theme.colors.textOnFillBrand};
        }
    }
`;

const GlossaryItemCardWrapper = styled.div`
    padding: 12px;
    border-radius: 13px;

    &:hover {
        transition: 0.15s;
        background-color: ${(props) => props.theme.colors.border};
    }
`;

const GlossaryItemCard = styled.div`
    display: flex;
    flex-direction: column;
    border-radius: 13px;
    border: 1px solid ${(props) => props.theme.colors.border};
    background: ${(props) => props.theme.colors.bg};
    transition: 0.15s;
    height: 100%;
    width: 100%;

    &:hover {
        transition: 0.15s;
        border-color: ${(props) => props.theme.styles['primary-color']};
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
    background: ${(props) => props.theme.colors.bgSurface};
    color: ${(props) => (props.count > 0 ? props.theme.colors.textTertiary : props.theme.colors.textDisabled)};
    padding: 5px 10px;
    width: max-content;
    svg {
        height: 14px;
        width: 14px;
        path {
            fill: ${(props) => (props.count > 0 ? props.theme.colors.textTertiary : props.theme.colors.textDisabled)};
        }
    }
    border: 1px solid transparent;
    :hover {
        border: 1px solid ${(props) => (props.count > 0 ? props.theme.colors.textTertiary : 'transparent')};
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
    background-color: ${(props) => props.theme.colors.overlayMedium};
`;

const GlossaryItemCardDetails = styled.div`
    display: flex;
    flex-direction: column;
    padding: 13px 16px;
    gap: 10px;
`;

const GlossaryCardHeader = styled(Typography)`
    color: ${(props) => props.theme.colors.textBrandOnBgFill};
    font-size: 16px;
    font-weight: 500;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
`;

const GlossaryItemCardDescription = styled(Typography)`
    color: ${(props) => props.theme.colors.textTertiary};
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
    const generateColor = useGenerateGlossaryColorFromPalette();
    const glossaryColor = displayProperties?.colorHex || generateColor(urn);

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
