import { Typography } from 'antd';
import React from 'react';
import styled from 'styled-components/macro';
import { GlossaryNodeFragment } from '../../graphql/fragments.generated';
import { ChildGlossaryTermFragment } from '../../graphql/glossaryNode.generated';
import { GlossaryNode, GlossaryTerm } from '../../types.generated';
import { useEntityData } from '../entity/shared/EntityContext';
import { REDESIGN_COLORS } from '../entityV2/shared/constants';
import { useEntityRegistry } from '../useEntityRegistry';
import GlossaryEntityItem from './GlossaryEntityItem';

const SectionTitle = styled(Typography)`
    margin: 12px 0 12px 16px;
    font-size: 12px;
    font-weight: 400;
    color: ${REDESIGN_COLORS.SUBTITLE};
`;

const GlossaryNodes = styled.div<{ isGrid?: boolean }>`
    display: flex;
    ${(props) =>
        props.isGrid
            ? `
        display: grid;
        grid-template-columns: repeat(auto-fill, minmax(24%, 1fr));
        gap: 8px; /* Adjust gap as needed */
        `
            : `
        display: flex;
        flex-direction: column;
        gap: 12px; /* Adjust gap as needed */
        `}
    width: 100%;
    margin-bottom: 20px;
`;

const GlossaryTerms = styled.div`
    display: flex;
    flex-direction: column;
    gap: 12px;
    margin-bottom: 20px;
`;

interface Props {
    nodes: (GlossaryNode | GlossaryNodeFragment)[];
    terms: (GlossaryTerm | ChildGlossaryTermFragment)[];
}

function GlossaryEntitiesList(props: Props) {
    const { nodes, terms } = props;
    const entityRegistry = useEntityRegistry();
    const { entityData } = useEntityData();
    const isGlossaryEntityPage = !!entityData;

    return (
        <>
            {nodes.length > 0 && isGlossaryEntityPage ? <SectionTitle>Term Groups</SectionTitle> : null}
            {nodes.length ? (
                <GlossaryNodes isGrid={!isGlossaryEntityPage}>
                    {nodes.map((node) => (
                        <GlossaryEntityItem
                            key={node.urn}
                            name={node.properties?.name || ''}
                            description={node.properties?.description || ''}
                            urn={node.urn}
                            type={node.type}
                            descendants={
                                (node as GlossaryNodeFragment).children?.relationships
                                    ?.map((child) => child.entity)
                                    .filter((child) => child !== null) as (GlossaryNode | GlossaryTerm)[]
                            }
                            displayProperties={node.displayProperties}
                            showAsCard={!isGlossaryEntityPage}
                        />
                    ))}
                </GlossaryNodes>
            ) : null}
            {terms.length > 0 && isGlossaryEntityPage ? <SectionTitle>Glossary Terms</SectionTitle> : null}
            {terms.length ? (
                <GlossaryTerms>
                    {terms.map((term) => (
                        <GlossaryEntityItem
                            key={term.urn}
                            name={entityRegistry.getDisplayName(term.type, term)}
                            urn={term.urn}
                            type={term.type}
                            description={term.properties?.description || ''}
                        />
                    ))}
                </GlossaryTerms>
            ) : null}
        </>
    );
}

export default GlossaryEntitiesList;
