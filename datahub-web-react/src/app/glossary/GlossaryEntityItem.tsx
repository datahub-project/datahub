import { FolderOutlined, RightOutlined } from '@ant-design/icons';
import { Maybe } from 'graphql/jsutils/Maybe';
import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components/macro';

import { useEntityRegistry } from '@app/useEntityRegistry';

import { EntityType } from '@types';

const ItemWrapper = styled.div`
    transition: 0.15s;

    &:hover {
        background-color: ${(props) => props.theme.colors.bgSurface};
    }
`;

const GlossaryItem = styled.div`
    align-items: center;
    border-bottom: 1px solid ${(props) => props.theme.colors.bgSurface};
    color: ${(props) => props.theme.colors.text};
    display: flex;
    font-size: 14px;
    font-weight: 700;
    justify-content: space-between;
    line-height: 22px;
    margin: 0 25px;
    padding: 16px 0;

    .anticon-folder {
        margin-right: 8px;
    }
`;

const StyledRightOutline = styled(RightOutlined)`
    color: ${(props) => props.theme.colors.textTertiary};
    font-size: 12px;
    margin-right: 20px;
`;

const CountWrapper = styled.span`
    color: ${(props) => props.theme.colors.textTertiary};
    font-size: 12px;
    font-weight: normal;
    margin-left: 7px;
`;

interface Props {
    name: string;
    urn: string;
    type: EntityType;
    count?: Maybe<number>;
}

function GlossaryEntityItem(props: Props) {
    const { name, urn, type, count } = props;

    const entityRegistry = useEntityRegistry();

    return (
        <Link to={`${entityRegistry.getEntityUrl(type, urn)}`}>
            <ItemWrapper>
                <GlossaryItem>
                    <span>
                        {type === EntityType.GlossaryNode && <FolderOutlined />}
                        {name}
                        <CountWrapper>{count}</CountWrapper>
                    </span>
                    {type === EntityType.GlossaryNode && <StyledRightOutline />}
                </GlossaryItem>
            </ItemWrapper>
        </Link>
    );
}

export default GlossaryEntityItem;
