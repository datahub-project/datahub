import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components/macro';

import { useEntityData } from '@app/entity/shared/EntityContext';
import GlossaryEntityItem from '@app/glossaryV2/GlossaryEntityItem';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { GlossaryNodeFragment, RootGlossaryNodeWithFourLayersFragment } from '@graphql/fragments.generated';
import { ChildGlossaryTermFragment } from '@graphql/glossaryNode.generated';
import { GlossaryNode, GlossaryTerm } from '@types';

const SectionTitle = styled.div`
    margin: 12px 0 12px 16px;
    font-size: 12px;
    font-weight: 400;
    color: ${(props) => props.theme.colors.text};
`;

const GlossaryNodes = styled.div<{ isGrid?: boolean }>`
    display: flex;
    ${(props) =>
        props.isGrid
            ? `
        display: grid;
        grid-template-columns: repeat(3, minmax(0, 1fr));
        gap: 8px;
        `
            : `
        display: flex;
        flex-direction: column;
        gap: 8px;
        padding: 0 16px;
        `}
    width: 100%;
    margin-bottom: 8px;
`;

const GlossaryTerms = styled.div`
    display: flex;
    flex-direction: column;
    gap: 8px;
    margin-bottom: 8px;
    padding: 0 16px;
`;

interface Props {
    nodes: (GlossaryNode | GlossaryNodeFragment | RootGlossaryNodeWithFourLayersFragment)[];
    terms: (GlossaryTerm | ChildGlossaryTermFragment)[];
}

function GlossaryEntitiesList(props: Props) {
    const { t } = useTranslation('governance.glossary');
    const { nodes, terms } = props;
    const entityRegistry = useEntityRegistry();
    const { entityData } = useEntityData();
    const isGlossaryEntityPage = !!entityData;

    return (
        <>
            {nodes.length > 0 && isGlossaryEntityPage ? <SectionTitle>{t('section.termGroups')}</SectionTitle> : null}
            {nodes.length ? (
                <GlossaryNodes isGrid={!isGlossaryEntityPage}>
                    {nodes.map((node) => (
                        <GlossaryEntityItem
                            key={node.urn}
                            name={node.properties?.name || ''}
                            description={node.properties?.description || ''}
                            urn={node.urn}
                            type={node.type}
                            displayProperties={node.displayProperties}
                            showAsCard={!isGlossaryEntityPage}
                            node={node}
                        />
                    ))}
                </GlossaryNodes>
            ) : null}
            {isGlossaryEntityPage && terms.length > 0 ? (
                <SectionTitle>{t('section.glossaryTerms')}</SectionTitle>
            ) : null}
            {isGlossaryEntityPage && terms.length ? (
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
