import React from 'react';
import Icon from '@ant-design/icons';
import { Typography } from 'antd';
import styled from 'styled-components/macro';
import { Maybe } from 'graphql/jsutils/Maybe';
import { Link } from 'react-router-dom';
import { EntityType } from '../../types.generated';
import { useEntityRegistry } from '../useEntityRegistry';
import { BusinessGlossaryEntitiesCardColors } from '../onboarding/config/BusinessGlossaryConfigV2';
import FolderIcon from '../../images/folder-open.svg?react';
import BookmarkIcon from '../../images/collections_bookmark.svg?react';
import ArrowRightIcon from '../../images/arrow_right_alt.svg?react';

const GlossaryItem = styled.div`
    align-items: center;
    color: #434863;
    display: flex;
    font-size: 14px;
    font-weight: 400;
    justify-content: space-between;
    line-height: normal;
    width: 100%;
    height: 100%;

    .anticon-folder {
        margin-right: 8px;
    }
`;

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

const GlossaryItemBadge = styled.span`
    position: absolute;
    left: -65px;
    top: 20px;
    width: 160px;
    transform: rotate(-45deg);
    padding: 10px;
    opacity: 1;
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
    opacity: 0.5;
    width: 100%;
    overflow: hidden;
    display: -webkit-box;
    -webkit-line-clamp: 2;
    -webkit-box-orient: vertical;
`;

const SmallDescription = styled(Typography)`
    color: rgba(86, 102, 142, 0.5);
    font-size: 10px;
    line-height: 16px;
    font-weight: 600;
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

const BookmarkIconWrapper = styled.div`
    border: 1px solid #d9d9d9;
    border-radius: 10px;
    backround: #fff;
    padding: 14px 11px 11px 13px;
`;

const EntityNameWrapper = styled.div`
    display: flex;
    flex-direction: column;
    gap: 2px;
`;

const EntityDetails = styled.div`
    display: flex;
    justify-content: space-between;
    align-items: center;
    border-bottom: 1px solid #f6f7fa;
    padding: 20px 0 20px 0;
    margin: 0 23px 0 19px;
`;

const EntityDetailsWrapper = styled.div`
    width: 100%;
    margin: 0 2px;

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
        background-color: #f6f7fa;
        border-radius: 4px;
    }
`;

const ItemWrapper = styled.div`
    transition: 0.15s;
    width: 100%;

    & a {
        display: block;
        width: 100%;
        height: 100%;
    }
`;

interface Props {
    name: string;
    description?: string;
    urn: string;
    type: EntityType;
    count?: Maybe<number>;
    index: number;
}

function GlossaryEntityItem(props: Props) {
    const { name, description, urn, type, count, index } = props;

    const entityRegistry = useEntityRegistry();

    return (
        <ItemWrapper style={{ flexBasis: type === EntityType.GlossaryNode ? '24%' : 'auto' }}>
            <Link to={`${entityRegistry.getEntityUrl(type, urn, { index: (index % 15).toString() })}`}>
                <GlossaryItem>
                    {type === EntityType.GlossaryNode ? (
                        <GlossaryItemCard>
                            <GlossaryItemCardHeader
                                index={index}
                            >
                                <GlossaryCardHeader>{name?.match(/\b(\w)/g)?.join('')}</GlossaryCardHeader>
                                <GlossaryItemBadge
                                    style={{ backgroundColor: `${BusinessGlossaryEntitiesCardColors[index % 15]}` }}
                                >
                                    {' '}
                                </GlossaryItemBadge>
                                {type === EntityType.GlossaryNode && (
                                    <GlossaryItemCount>
                                        <CountWrapper>
                                            <Icon component={FolderIcon} style={{ fontSize: '17px' }} />
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
                    ) : (
                        <EntityDetailsWrapper>
                            <EntityDetails>
                                <EntityDetailsLeftColumn>
                                    <BookmarkIconWrapper>
                                        <BookmarkIcon />
                                    </BookmarkIconWrapper>
                                    <EntityNameWrapper>
                                        {name}
                                        <SmallDescription>Amount of money available or owed</SmallDescription>
                                    </EntityNameWrapper>
                                </EntityDetailsLeftColumn>
                                <EntityDetailsRightColumn>
                                    <ArrowRightIcon />
                                </EntityDetailsRightColumn>
                            </EntityDetails>
                        </EntityDetailsWrapper>
                    )}
                </GlossaryItem>
            </Link>
        </ItemWrapper>
    );
}

export default GlossaryEntityItem;
