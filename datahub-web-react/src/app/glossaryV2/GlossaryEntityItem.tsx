import React from 'react';
import styled from 'styled-components/macro';
import { Maybe } from 'graphql/jsutils/Maybe';
import { Link } from 'react-router-dom';
import { EntityType } from '../../types.generated';
import { useEntityRegistry } from '../useEntityRegistry';
import GlossaryNodeCard from './GlossaryNodeCard';
import GlossaryTermItem from './GlossaryTermItem';

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

interface ItemWrapperProps {
    type: EntityType;
}

const ItemWrapper = styled.div<ItemWrapperProps>`
    transition: 0.15s;
    width: 100%;
    flex-basis: ${(props) => (props.type === EntityType.GlossaryNode ? '24%' : 'auto')};

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
        <ItemWrapper type={type}>
            <Link to={`${entityRegistry.getEntityUrl(type, urn, { index: (index % 15).toString() })}`}>
                <GlossaryItem>
                    {type === EntityType.GlossaryNode ? (
                        <GlossaryNodeCard
                            index={index}
                            name={name}
                            type={type}
                            description={description}
                            count={count}
                        />
                    ) : (
                        <GlossaryTermItem name={name} description={description} />
                    )}
                </GlossaryItem>
            </Link>
        </ItemWrapper>
    );
}

export default GlossaryEntityItem;
