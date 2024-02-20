import React from 'react';
import styled from 'styled-components/macro';
import { Typography } from 'antd';
import { GlossaryNodeFragment } from '../../graphql/fragments.generated';
import { ChildGlossaryTermFragment } from '../../graphql/glossaryNode.generated';
import { EntityType, GlossaryNode, GlossaryTerm } from '../../types.generated';
import { useEntityRegistry } from '../useEntityRegistry';
import GlossaryEntityItem from './GlossaryEntityItem';

interface GlossaryEntityWrapperProps {
    termsTotal?: number | undefined;
}

const GlossaryEntityWrapper = styled.div<GlossaryEntityWrapperProps>`
    overflow: auto;
    height: ${(props) => (props.termsTotal ? '70vh' : '80vh')};
`;

interface EntitiesWrapperProps {
    type: EntityType;
}

const EntitiesWrapper = styled.div<EntitiesWrapperProps>`
    display: flex;
    overflow: auto;
    flex-wrap: wrap;
    padding: ${(props) => (props.type === EntityType.GlossaryNode ? '25px 29px' : 0)};
    gap: ${(props) => (props.type === EntityType.GlossaryNode ? '14px' : 'unset')};
`;

const EntityTitle = styled(Typography)`
    margin: 11px 0 12px 19px;
    font-size: 12px;
    font-weight: 400;
    color: #434863;
`;

interface Props {
    nodes: (GlossaryNode | GlossaryNodeFragment)[];
    terms: (GlossaryTerm | ChildGlossaryTermFragment)[];
    termsTotal: number | undefined;
}

function GlossaryEntitiesList(props: Props) {
    const { nodes, terms, termsTotal } = props;
    const entityRegistry = useEntityRegistry();

    return (
        <GlossaryEntityWrapper termsTotal={termsTotal}>
            <EntitiesWrapper type={nodes[0]?.type}>
                {nodes[0]?.type !== EntityType.GlossaryNode && <EntityTitle>Glossary Terms</EntityTitle>}
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
