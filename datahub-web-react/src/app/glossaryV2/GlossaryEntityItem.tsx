import React from 'react';
import styled from 'styled-components/macro';
import { Maybe } from 'graphql/jsutils/Maybe';
import { Link } from 'react-router-dom';
import { DisplayProperties, EntityType } from '../../types.generated';
import { useEntityRegistry } from '../useEntityRegistry';
import GlossaryNodeCard from './GlossaryNodeCard';
import GlossaryTermItem from './GlossaryTermItem';
import { useEntityData } from '../entity/shared/EntityContext';
import { GenericEntityProperties } from '../entity/shared/types';
import { REDESIGN_COLORS } from '../entityV2/shared/constants';

const GlossaryItem = styled.div`
    align-items: center;
    color: ${REDESIGN_COLORS.SUBTITLE};
    font-size: 14px;
    font-weight: 400;
    line-height: normal;
    width: 100%;
    height: 100%;
    position: relative;
    overflow: hidden;
    padding: 12px 20px 20px 32px;
    border-top: 1px solid rgba(0, 0, 0, 0.1);

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
    min-width: 200px;
    & a {
        display: block;
        width: 100%;
        height: 100%;
    }
    &:hover {
        transition: 0.15s;
        background-color: ${REDESIGN_COLORS.LIGHT_GREY};
        border-radius: 4px;
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
    const { name, description, urn, type, count, displayProperties } = props;
    const entityRegistry = useEntityRegistry();
    const entityData = useEntityData();

    return (
        <ItemWrapper type={type} entityData={entityData}>
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
                        <GlossaryTermItem
                            name={name}
                            description={description}
                            type={type}
                            urn={urn}
                            entityData={entityData}
                            count={count}
                        />
                    )}
                </GlossaryItem>
            </Link>
        </ItemWrapper>
    );
}

export default GlossaryEntityItem;
