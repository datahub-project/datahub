import React from 'react';
import styled from 'styled-components/macro';
import { Maybe } from 'graphql/jsutils/Maybe';
import { Link } from 'react-router-dom';
import { DisplayProperties, EntityType } from '../../types.generated';
import { useEntityRegistry } from '../useEntityRegistry';
import GlossaryNodeCard from './GlossaryNodeCard';
import GlossaryTermItem from './GlossaryTermItem';
import { useEntityData } from '../entityV2/shared/EntityContext';
import { GenericEntityProperties } from '../entityV2/shared/types';
import { REDESIGN_COLORS } from '../entityV2/shared/constants';

const GlossaryItem = styled.div`
    align-items: center;
    color: ${REDESIGN_COLORS.SUBTITLE};
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
    entityData: {
        urn: string;
        entityType: EntityType;
        entityData: GenericEntityProperties | null;
        loading: boolean;
    };
}

const ItemWrapper = styled.div<ItemWrapperProps>`
    transition: 0.15s;
    width: 100%;
    flex-basis: ${(props) => (props.type === EntityType.GlossaryNode && !props.entityData.urn ? '24%' : 'auto')};

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
    displayProperties?: Maybe<DisplayProperties>;
}

function GlossaryEntityItem(props: Props) {
    const { name, description, urn, type, count, displayProperties} = props;
    const entityRegistry = useEntityRegistry();
    const entityData = useEntityData();

    return (
        <ItemWrapper type={type} entityData={entityData} >
            <Link to={`${entityRegistry.getEntityUrl(type, urn)}`}>
                <GlossaryItem>
                    {type === EntityType.GlossaryNode && !entityData.urn ? (
                        <GlossaryNodeCard
                            name={name}
                            type={type}
                            description={description}
                            count={count}
                            displayProperties={displayProperties}
                            urn={urn}
                        />
                    ) : (
                        <GlossaryTermItem name={name} description={description} type={type}/>
                    )}
                </GlossaryItem>
            </Link>
        </ItemWrapper>
    );
}

export default GlossaryEntityItem;
