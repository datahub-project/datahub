import React from 'react';
import Icon from '@ant-design/icons';
import { Typography } from 'antd';
import styled from 'styled-components/macro';
import FolderIcon from '../../images/folder-open.svg?react';
import { BusinessGlossaryEntitiesCardColors } from '../onboarding/config/BusinessGlossaryConfigV2';
import { Maybe } from 'graphql/jsutils/Maybe';
import { EntityType } from '../../types.generated';

interface GlossaryItemCardHeaderProps {
    index: number;
}

const GlossaryItemCardHeader = styled.div<GlossaryItemCardHeaderProps>`
    display: flex;
    padding: 40px 0 30px;
    justify-content: center;
    border-radius: 12px;
    position: relative;
    overflow: hidden;
    opacity: 0.7;
    background-color: ${(props) => `${BusinessGlossaryEntitiesCardColors[props.index % 15]}`};
`;

const GlossaryItemCountDiv = styled.div`
    position: absolute;
    top: -7px;
    right: -3px;
    border-radius: 7px;
    width: 12px;
    height: 11px;
    background: #3cb47a;
    font-size: 8px;
    color: #fff;
    text-align: center;
    display: none;
`;

const GlossaryItemCount = styled.span`
    position: absolute;
    right: 1px;
    bottom: 1px;
    border-radius: 12px 0px 11px 1px;
    background: #fff;
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
    border: 1px solid #ededed;
    background: #fff;
    transition: 0.15s;
    height: 100%;
    width: 100%;

    &:hover {
        transition: 0.15s;
        border-color: #5c3fd1;
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
    index: number;
}

const GlossaryItemBadge = styled.span<GlossaryItemBadgeProps>`
    position: absolute;
    left: -65px;
    top: 20px;
    width: 160px;
    transform: rotate(-45deg);
    padding: 10px;
    opacity: 1;
    background-color: ${(props) => `${BusinessGlossaryEntitiesCardColors[props.index % 15]}`};
`;

const GlossaryItemCardDetails = styled.div`
    display: flex;
    flex-direction: column;
    padding: 13px 16px;
`;

const GlossaryCardHeader = styled(Typography)`
    color: #fff;
    font-size: 44px;
`;

const GlossaryItemCardTitle = styled(Typography)`
    color: #434863;
    font-size: 14px;
    font-weight: 400;
`;

const GlossaryItemCardDescription = styled(Typography)`
    color: #434863;
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
    index: number;
    name: string;
    type: EntityType;
    description: string | undefined;
    count?: Maybe<number>;
}

const GlossaryNodeCard = (props: Props) => {
    const { index, name, type, description, count } = props;

    return (
        <GlossaryItemCard>
            <GlossaryItemCardHeader index={index}>
                <GlossaryCardHeader>{name?.match(/\b(\w)/g)?.join('')}</GlossaryCardHeader>
                <GlossaryItemBadge index={index} />
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
