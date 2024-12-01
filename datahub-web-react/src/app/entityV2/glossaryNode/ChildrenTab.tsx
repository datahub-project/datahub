import React from 'react';
import styled from 'styled-components';

import { useEntityData } from '@app/entity/shared/EntityContext';
import useGlossaryChildren from '@app/entityV2/glossaryNode/useGlossaryChildren';
import { sortGlossaryNodes } from '@app/entityV2/glossaryNode/utils';
import { sortGlossaryTerms } from '@app/entityV2/glossaryTerm/utils';
import EmptyGlossarySection from '@app/glossaryV2/EmptyGlossarySection';
import GlossaryEntitiesList from '@app/glossaryV2/GlossaryEntitiesList';
import { useEntityRegistry } from '@app/useEntityRegistry';
import { colors } from '@src/alchemy-components';
import { SearchBar } from '@src/app/searchV2/SearchBar';
import Loading from '@src/app/shared/Loading';

import { EntityType, GlossaryNode, GlossaryTerm } from '@types';

const ChildrenTabWrapper = styled.div`
    height: 100%;
    overflow: auto;
    padding-bottom: 10px;
`;

const LoadingWrapper = styled.div`
    height: 100px;
    display: flex;
    align-items: center;
    justify-content: center;
`;

function ChildrenTab() {
    const { entityData } = useEntityData();
    const entityRegistry = useEntityRegistry();
    const entityUrn = entityData?.urn;
    const { scrollRef, data, loading, searchQuery, setSearchQuery } = useGlossaryChildren({ entityUrn });

    if (!entityData) return <></>;

    const childNodes = data
        .filter((child) => child.type === EntityType.GlossaryNode)
        .sort((nodeA, nodeB) => sortGlossaryNodes(entityRegistry, nodeA, nodeB));
    const childTerms = data
        .filter((child) => child.type === EntityType.GlossaryTerm)
        .sort((termA, termB) => sortGlossaryTerms(entityRegistry, termA, termB));

    const hasTermsOrNodes = !!childNodes?.length || !!childTerms?.length;

    if (searchQuery || hasTermsOrNodes) {
        return (
            <ChildrenTabWrapper>
                <SearchBar
                    placeholderText="Search contents..."
                    onQueryChange={setSearchQuery}
                    onSearch={() => null}
                    suggestions={[]}
                    hideRecommendations
                    entityRegistry={entityRegistry}
                    textColor={colors.gray[800]}
                    placeholderColor={colors.gray[300]}
                    style={{ padding: 16, margin: 0 }}
                />
                <GlossaryEntitiesList
                    nodes={(childNodes as GlossaryNode[]) || []}
                    terms={(childTerms as GlossaryTerm[]) || []}
                />
                {loading && (
                    <LoadingWrapper>
                        <Loading marginTop={0} height={24} />
                    </LoadingWrapper>
                )}
                <div ref={scrollRef} />
            </ChildrenTabWrapper>
        );
    }

    return <EmptyGlossarySection description="No Terms or Term Groups" />;
}

export default ChildrenTab;
