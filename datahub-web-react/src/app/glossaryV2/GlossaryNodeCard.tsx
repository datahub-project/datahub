import React from 'react';
import Icon from '@ant-design/icons';
import { Typography } from 'antd';
import styled from 'styled-components/macro';
import { Maybe } from 'graphql/jsutils/Maybe';
import FolderIcon from '../../images/folder-open.svg?react';
import { DisplayProperties, EntityType } from '../../types.generated';
import { generateColor } from '../entityV2/shared/components/styled/StyledTag';
import { hexToRgba } from '../entityV2/shared/links/colorUtils';
import { REDESIGN_COLORS, ANTD_GRAY } from '../entityV2/shared/constants';

interface GlossaryItemCardHeaderProps {
    color: string;
}

const GlossaryItemCardHeader = styled.div<GlossaryItemCardHeaderProps>`
    display: flex;
    padding: 40px 0 30px;
    justify-content: center;
    border-radius: 12px;
    position: relative;
    overflow: hidden;
    background-color: ${(props) => hexToRgba(props.color, 0.7)};
`;

const GlossaryItemCountDiv = styled.div`
    position: absolute;
    top: -7px;
    right: -3px;
    border-radius: 7px;
    width: 12px;
    height: 11px;
    background: ${REDESIGN_COLORS.TERTIARY_GREEN};
    font-size: 8px;
    color: ${ANTD_GRAY[1]};
    text-align: center;
    display: none;
`;

const GlossaryItemCount = styled.span`
    position: absolute;
    right: 1px;
    bottom: 1px;
    border-radius: 12px 0px 11px 1px;
    background: ${ANTD_GRAY[1]};
    padding: 10px;
`;

const CountWrapper = styled.span`
    position: relative;
`;

const StyledIcon = styled(Icon)`
    font-size: 17px;
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

    &:hover > ${GlossaryItemCardHeader} > ${GlossaryItemCount} > ${CountWrapper} > ${GlossaryItemCountDiv} {
        transition: 0.15s;
        display: block;
    }
`;

interface GlossaryItemBadgeProps {
    color: string;
}

const GlossaryItemBadge = styled.span<GlossaryItemBadgeProps>`
    position: absolute;
    left: -65px;
    top: 20px;
    width: 160px;
    transform: rotate(-45deg);
    padding: 10px;
    opacity: 1;
    background-color: ${(props) => `${props.color}`};
`;

const GlossaryItemCardDetails = styled.div`
    display: flex;
    flex-direction: column;
    padding: 13px 16px;
`;

const GlossaryCardHeader = styled(Typography)`
    color: ${ANTD_GRAY[1]};
    font-size: 44px;
`;

const GlossaryItemCardTitle = styled(Typography)`
    color: ${REDESIGN_COLORS.SUBTITLE};
    font-size: 14px;
    font-weight: 400;
`;

const GlossaryItemCardDescription = styled(Typography)`
    color: ${REDESIGN_COLORS.SUBTITLE};
    font-size: 10px;
    line-height: 13px;
    font-weight: 400;
    margin-top: 1px;
    opacity: 0.5;
    width: 100%;
    overflow: hidden;
    display: -webkit-box;
    -webkit-line-clamp: 2;
    -webkit-box-orient: vertical;
`;

interface Props {
    name: string;
    type: EntityType;
    description: string | undefined;
    count?: Maybe<number>;
    displayProperties?: Maybe<DisplayProperties>;
    urn: string;
}

const GlossaryNodeCard = (props: Props) => {
    const { name, type, description, count, displayProperties, urn } = props;
    const glossaryColor = displayProperties?.colorHex || generateColor.hex(urn);

    return (
        <GlossaryItemCard>
            <GlossaryItemCardHeader color={glossaryColor}>
                <GlossaryCardHeader>{name?.match(/\b(\w)/g)?.join('')}</GlossaryCardHeader>
                <GlossaryItemBadge color={glossaryColor} />
                {type === EntityType.GlossaryNode && (
                    <GlossaryItemCount>
                        <CountWrapper>
                            <StyledIcon component={FolderIcon} />
                            <GlossaryItemCountDiv>{count}</GlossaryItemCountDiv>
                        </CountWrapper>
                    </GlossaryItemCount>
                )}
            </GlossaryItemCardHeader>
            <GlossaryItemCardDetails>
                <GlossaryItemCardTitle>{name}</GlossaryItemCardTitle>
                <GlossaryItemCardDescription>{description}</GlossaryItemCardDescription>
            </GlossaryItemCardDetails>
        </GlossaryItemCard>
    );
};

export default GlossaryNodeCard;
