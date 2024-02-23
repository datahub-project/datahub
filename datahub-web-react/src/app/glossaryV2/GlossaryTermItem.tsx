import React from 'react';
import { Typography } from 'antd';
import styled from 'styled-components/macro';
import BookmarkIcon from '../../images/collections_bookmark.svg?react';
import ArrowRightIcon from '../../images/arrow_right_alt.svg?react';

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

interface Props {
    name: string;
    description: string | undefined;
}

const GlossaryTermItem = (props: Props) => {
    const { name, description } = props;

    return (
        <EntityDetailsWrapper>
            <EntityDetails>
                <EntityDetailsLeftColumn>
                    <BookmarkIconWrapper>
                        <BookmarkIcon />
                    </BookmarkIconWrapper>
                    <EntityNameWrapper>
                        {name}
                        {description && <SmallDescription>{description}</SmallDescription>}
                    </EntityNameWrapper>
                </EntityDetailsLeftColumn>
                <EntityDetailsRightColumn>
                    <ArrowRightIcon />
                </EntityDetailsRightColumn>
            </EntityDetails>
        </EntityDetailsWrapper>
    );
};

export default GlossaryTermItem;
