import React from 'react';

import { sortGlossaryNodes } from '@app/entity/glossaryNode/utils';
import { sortGlossaryTerms } from '@app/entity/glossaryTerm/utils';
import { useEntityData } from '@app/entity/shared/EntityContext';
import EmptyGlossarySection from '@app/glossary/EmptyGlossarySection';
import GlossaryEntitiesList from '@app/glossary/GlossaryEntitiesList';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { EntityType, GlossaryNode, GlossaryTerm } from '@types';

function ChildrenTab() {
    const { entityData } = useEntityData();
    const entityRegistry = useEntityRegistry();

    if (!entityData) return <></>;

    const childNodes = entityData?.children?.relationships
        .filter((child) => child.entity?.type === EntityType.GlossaryNode)
        .sort((nodeA, nodeB) => sortGlossaryNodes(entityRegistry, nodeA.entity, nodeB.entity))
        .map((child) => child.entity);
    const childTerms = entityData?.children?.relationships
        .filter((child) => child.entity?.type === EntityType.GlossaryTerm)
        .sort((termA, termB) => sortGlossaryTerms(entityRegistry, termA.entity, termB.entity))
        .map((child) => child.entity);

    const hasTermsOrNodes = !!childNodes?.length || !!childTerms?.length;

    if (hasTermsOrNodes) {
        return (
            <GlossaryEntitiesList
                nodes={(childNodes as GlossaryNode[]) || []}
                terms={(childTerms as GlossaryTerm[]) || []}
            />
        );
    }

    return <EmptyGlossarySection description="No Terms or Term Groups" />;
}

export default ChildrenTab;
