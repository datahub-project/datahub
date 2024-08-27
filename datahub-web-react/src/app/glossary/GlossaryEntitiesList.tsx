import React from 'react';
import styled from 'styled-components/macro';
import { GlossaryNodeFragment } from '../../graphql/fragments.generated';
import { ChildGlossaryTermFragment } from '../../graphql/glossaryNode.generated';
import { GlossaryNode, GlossaryTerm } from '../../types.generated';
import { useEntityRegistry } from '../useEntityRegistry';
import GlossaryEntityItem from './GlossaryEntityItem';

const EntitiesWrapper = styled.div`
    flex: 1;
    overflow: auto;
    padding-bottom: 20px;
`;

interface Props {
    nodes: (GlossaryNode | GlossaryNodeFragment)[];
    terms: (GlossaryTerm | ChildGlossaryTermFragment)[];
}

function GlossaryEntitiesList(props: Props) {
    const { nodes, terms } = props;
    const entityRegistry = useEntityRegistry();

    return (
        <EntitiesWrapper>
            {nodes.map((node) => (
                <GlossaryEntityItem
                    name={node.properties?.name || ''}
                    urn={node.urn}
                    type={node.type}
                    count={(node as GlossaryNodeFragment).children?.total}
                />
            ))}
            {terms.map((term) => (
                <GlossaryEntityItem
                    name={entityRegistry.getDisplayName(term.type, term)}
                    urn={term.urn}
                    type={term.type}
                />
            ))}
        </EntitiesWrapper>
    );
}

export default GlossaryEntitiesList;
