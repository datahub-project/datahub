import React from 'react';
import { FolderOutlined, RightOutlined } from '@ant-design/icons';
import styled from 'styled-components/macro';
import { Maybe } from 'graphql/jsutils/Maybe';
import { Link } from 'react-router-dom';
import { EntityType } from '../../types.generated';
import { ANTD_GRAY } from '../entity/shared/constants';
import { useEntityRegistry } from '../useEntityRegistry';

const ItemWrapper = styled.div`
    transition: 0.15s;

    &:hover {
        background-color: ${ANTD_GRAY[3]};
    }
`;

const GlossaryItem = styled.div`
    align-items: center;
    border-bottom: 1px solid ${ANTD_GRAY[4]};
    color: #262626;
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
    color: ${ANTD_GRAY[7]};
    font-size: 12px;
    margin-right: 20px;
`;

const CountWrapper = styled.span`
    color: ${ANTD_GRAY[7]};
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
