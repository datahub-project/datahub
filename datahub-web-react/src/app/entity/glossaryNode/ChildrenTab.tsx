/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';
import styled from 'styled-components';

import { sortGlossaryNodes } from '@app/entity/glossaryNode/utils';
import { sortGlossaryTerms } from '@app/entity/glossaryTerm/utils';
import { useEntityData } from '@app/entity/shared/EntityContext';
import useGlossaryChildren from '@app/entityV2/glossaryNode/useGlossaryChildren';
import EmptyGlossarySection from '@app/glossary/EmptyGlossarySection';
import GlossaryEntitiesList from '@app/glossary/GlossaryEntitiesList';
import { useEntityRegistry } from '@app/useEntityRegistry';
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
    const { scrollRef, data, loading } = useGlossaryChildren({ entityUrn });

    if (!entityData) return <></>;

    const childNodes = data
        .filter((child) => child.type === EntityType.GlossaryNode)
        .sort((nodeA, nodeB) => sortGlossaryNodes(entityRegistry, nodeA, nodeB));
    const childTerms = data
        .filter((child) => child.type === EntityType.GlossaryTerm)
        .sort((termA, termB) => sortGlossaryTerms(entityRegistry, termA, termB));

    const hasTermsOrNodes = !!childNodes?.length || !!childTerms?.length;

    if (hasTermsOrNodes) {
        return (
            <ChildrenTabWrapper>
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
