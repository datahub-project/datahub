import React from 'react';
import styled from 'styled-components/macro';
import { Typography } from 'antd';
import { GlossaryNodeFragment } from '../../graphql/fragments.generated';
import { ChildGlossaryTermFragment } from '../../graphql/glossaryNode.generated';
import { EntityType, GlossaryNode, GlossaryTerm } from '../../types.generated';
import { useEntityRegistry } from '../useEntityRegistry';
import GlossaryEntityItem from './GlossaryEntityItem';
import { useEntityData } from '../entity/shared/EntityContext';
import { GenericEntityProperties } from '../entity/shared/types';
import { REDESIGN_COLORS } from '../entityV2/shared/constants';

interface GlossaryEntityWrapperProps {
    termsTotal?: number | undefined;
}

const GlossaryEntityWrapper = styled.div<GlossaryEntityWrapperProps>`
    overflow: auto;
    height: ${(props) => (props.termsTotal ? '70vh' : '80vh')};
`;

interface EntitiesWrapperProps {
    type: EntityType;
    entityData: {
        urn: string;
        entityType: EntityType;
        entityData: GenericEntityProperties | null;
        loading: boolean;
    };
}

const EntitiesWrapper = styled.div<EntitiesWrapperProps>`
    display: flex;
    overflow: auto;
    flex-wrap: wrap;
    padding: ${(props) => (props.type === EntityType.GlossaryNode && !props.entityData.urn ? '25px 29px' : 0)};
    gap: ${(props) => (props.type === EntityType.GlossaryNode && !props.entityData.urn ? '14px' : 'unset')};
`;

const EntityTitle = styled(Typography)`
    margin: 11px 0 12px 19px;
    font-size: 12px;
    font-weight: 400;
    color: ${REDESIGN_COLORS.SUBTITLE};
`;

interface Props {
    nodes: (GlossaryNode | GlossaryNodeFragment)[];
    terms: (GlossaryTerm | ChildGlossaryTermFragment)[];
    termsTotal?: number;
}

function GlossaryEntitiesList(props: Props) {
    const { nodes, terms, termsTotal } = props;
    const entityRegistry = useEntityRegistry();
    const entityData = useEntityData();

    return (
        <GlossaryEntityWrapper termsTotal={termsTotal}>
            <EntitiesWrapper type={nodes[0]?.type} entityData={entityData}>
                {nodes.length > 0&& entityData.urn !== '' && <EntityTitle>Terms Group</EntityTitle>}
                {nodes.map((node) => (
                    <GlossaryEntityItem
                        key={node.urn}
                        name={node.properties?.name || ''}
                        description={node.properties?.description || ''}
                        urn={node.urn}
                        type={node.type}
                        count={(node as GlossaryNodeFragment).children?.total}
                        displayProperties={node.displayProperties}
                    />
                ))}
                { entityData.urn !== '' &&<EntityTitle>Glossary Terms</EntityTitle>}
                {terms.map((term) => (
                    <GlossaryEntityItem
                        key={term.urn}
                        name={entityRegistry.getDisplayName(term.type, term)}
                        urn={term.urn}
                        type={term.type}
                    />
                ))}
            </EntitiesWrapper>
        </GlossaryEntityWrapper>
    );
}

export default GlossaryEntitiesList;
