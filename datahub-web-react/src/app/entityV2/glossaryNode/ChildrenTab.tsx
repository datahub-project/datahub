import React from 'react';

import { useEntityData } from '@app/entity/shared/EntityContext';
import { sortGlossaryNodes } from '@app/entityV2/glossaryNode/utils';
import { sortGlossaryTerms } from '@app/entityV2/glossaryTerm/utils';
import EmptyGlossarySection from '@app/glossaryV2/EmptyGlossarySection';
import GlossaryEntitiesList from '@app/glossaryV2/GlossaryEntitiesList';
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
