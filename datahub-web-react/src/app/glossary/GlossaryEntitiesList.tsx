import React from 'react';
import { Empty } from 'antd';
import styled from 'styled-components/macro';
import { GlossaryNodeFragment } from '../../graphql/fragments.generated';
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
    terms: GlossaryTerm[];
}

function GlossaryEntitiesList(props: Props) {
    const { nodes, terms } = props;
    const entityRegistry = useEntityRegistry();

    const contentsData =
        nodes.length === 0 && terms.length === 0 ? (
            <Empty description="No Terms or Term Groups!" image={Empty.PRESENTED_IMAGE_SIMPLE} />
        ) : (
            <EntitiesWrapper>
                {nodes.map((node) => (
                    <GlossaryEntityItem
                        name={node.properties?.name || ''}
                        urn={node.urn}
                        type={node.type}
                        count={(node as GlossaryNodeFragment).children?.count}
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
    return contentsData;
}

export default GlossaryEntitiesList;
