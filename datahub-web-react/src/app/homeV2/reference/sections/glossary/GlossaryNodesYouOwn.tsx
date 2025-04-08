import React, { useState } from 'react';

import { useUserContext } from '@app/context/useUserContext';
import { EmbeddedListSearchModal } from '@app/entityV2/shared/components/styled/search/EmbeddedListSearchModal';
import { GlossaryTermMiniPreview } from '@app/entityV2/shared/links/GlossaryTermMiniPreview';
import { ReferenceSection } from '@app/homeV2/layout/shared/styledComponents';
import { EntityLinkList } from '@app/homeV2/reference/sections/EntityLinkList';
import { EmptyGlossaryNodesYouOwn } from '@app/homeV2/reference/sections/glossary/EmptyGlossaryNodesYouOwn';
import { useGetGlossaryNodesYouOwn } from '@app/homeV2/reference/sections/glossary/useGetGlossaryNodesYouOwn';
import { ReferenceSectionProps } from '@app/homeV2/reference/types';
import { ENTITY_FILTER_NAME, OWNERS_FILTER_NAME, UnionType } from '@app/searchV2/utils/constants';

import { EntityType, GlossaryTerm } from '@types';

const DEFAULT_MAX_ENTITIES_TO_SHOW = 5;

export const GlossaryNodesYouOwn = ({ hideIfEmpty }: ReferenceSectionProps) => {
    const userContext = useUserContext();
    const { user } = userContext;
    const [entityCount, setEntityCount] = useState(DEFAULT_MAX_ENTITIES_TO_SHOW);
    const [showModal, setShowModal] = useState(false);
    const { entities, loading } = useGetGlossaryNodesYouOwn(user);

    if (hideIfEmpty && entities.length === 0) {
        return null;
    }

    return (
        <ReferenceSection>
            <EntityLinkList
                loading={loading || !user}
                entities={entities.slice(0, entityCount)}
                title="Your glossary terms"
                tip="Glossary Terms that you own"
                showMore={entities.length > entityCount}
                showMoreCount={
                    entityCount + DEFAULT_MAX_ENTITIES_TO_SHOW > entities.length
                        ? entities.length - entityCount
                        : DEFAULT_MAX_ENTITIES_TO_SHOW
                }
                onClickMore={() => setEntityCount(entityCount + DEFAULT_MAX_ENTITIES_TO_SHOW)}
                onClickTitle={() => setShowModal(true)}
                empty={<EmptyGlossaryNodesYouOwn />}
                render={(entity) => <GlossaryTermMiniPreview glossaryTerm={entity as GlossaryTerm} />}
            />
            {showModal && (
                <EmbeddedListSearchModal
                    title="Your glossary"
                    fixedFilters={{
                        unionType: UnionType.AND,
                        filters: [
                            { field: OWNERS_FILTER_NAME, values: [user?.urn as string] },
                            { field: ENTITY_FILTER_NAME, values: [EntityType.GlossaryNode, EntityType.GlossaryTerm] },
                        ],
                    }}
                    onClose={() => setShowModal(false)}
                    placeholderText="Filter glossary terms and groups..."
                />
            )}
        </ReferenceSection>
    );
};
