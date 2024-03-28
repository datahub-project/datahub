import React from 'react';
import { Typography } from 'antd';
import styled from 'styled-components/macro';
import { Maybe } from 'graphql/jsutils/Maybe';
import TermGroupIcon from '../../images/glossary_collections_bookmark.svg?react';
import TermIcon from '../../images/collections_bookmark.svg?react';
import { DisplayProperties, EntityType } from '../../types.generated';
import { generateColor } from '../entityV2/shared/components/styled/StyledTag';
import { hexToRgba } from '../entityV2/shared/links/colorUtils';
import { REDESIGN_COLORS, ANTD_GRAY, ANTD_GRAY_V2 } from '../entityV2/shared/constants';

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
    background-color: ${(props) => hexToRgba(props.color, 0.7)};

    svg {
        height: 22px;
        width: 22px;
        path {
            fill: white;
        }
    }
`;

const GlossaryItemCount = styled.span`
    display: flex;
    align-items: center;
    gap: 5px;
    border-radius: 20px;
    background: ${ANTD_GRAY_V2[14]};
    padding: 5px 10px;
    width: max-content;
    svg {
        height: 14px;
        width: 14px;
        path {
            fill: ${REDESIGN_COLORS.SUB_TEXT};
        }
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

interface GlossaryItemBadgeProps {
    color: string;
}

const GlossaryItemBadge = styled.span<GlossaryItemBadgeProps>`
    position: absolute;
    left: -35px;
    top: 8px;
    width: 100px;
    transform: rotate(-45deg);
    padding: 8px;
    opacity: 1;
    background-color: ${(props) => `${props.color}`};
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

const CountText = styled(Typography.Text)`
    color: ${REDESIGN_COLORS.SUB_TEXT};
    font-size: 10px;
    font-weight: 400;
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
                <TermGroupIcon />
                <GlossaryCardHeader>{name}</GlossaryCardHeader>
                <GlossaryItemBadge color={glossaryColor} />
            </GlossaryItemCardHeader>
            <GlossaryItemCardDetails>
                <GlossaryItemCardDescription>{description || '--'}</GlossaryItemCardDescription>
                {type === EntityType.GlossaryNode && (
                    <GlossaryItemCount>
                        <TermIcon />
                        <CountText> {count} </CountText>
                    </GlossaryItemCount>
                )}
            </GlossaryItemCardDetails>
        </GlossaryItemCard>
    );
};

export default GlossaryNodeCard;
