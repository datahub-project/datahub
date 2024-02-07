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

const EntitiesWrapper = styled.div`
    display: flex;
    overflow: auto;
    flex-wrap: wrap;
`;

const EntityTitle = styled(Typography)`
    margin: 11px 0 0 19px;
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
            <EntitiesWrapper>
                {nodes.map((node, index) => (
                    <GlossaryEntityItem
                        key={node.urn}
                        name={node.properties?.name || ''}
                        description={node.properties?.description || ''}
                        urn={node.urn}
                        type={node.type}
                        count={(node as GlossaryNodeFragment).children?.total}
                        index={index}
                    />
                ))}
                {terms.map((term, index) => (
                    <GlossaryEntityItem
                        key={term.urn}
                        name={entityRegistry.getDisplayName(term.type, term)}
                        urn={term.urn}
                        type={term.type}
                        index={index}
                    />
                ))}
            </EntitiesWrapper>
        </GlossaryEntityWrapper>
    );
}

export default GlossaryEntitiesList;
