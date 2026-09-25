import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';

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

export const GlossaryNodesYouOwn = ({ hideIfEmpty, trackClickInSection }: ReferenceSectionProps) => {
    const { t } = useTranslation('home.v2');
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
                title={t('yourGlossary.title')}
                tip={t('yourGlossary.tip')}
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
                onClickEntity={trackClickInSection}
            />
            {showModal && (
                <EmbeddedListSearchModal
                    title={t('yourGlossary.modalTitle')}
                    fixedFilters={{
                        unionType: UnionType.AND,
                        filters: [
                            { field: OWNERS_FILTER_NAME, values: [user?.urn as string] },
                            { field: ENTITY_FILTER_NAME, values: [EntityType.GlossaryNode, EntityType.GlossaryTerm] },
                        ],
                    }}
                    onClose={() => setShowModal(false)}
                    placeholderText={t('yourGlossary.filterPlaceholder')}
                />
            )}
        </ReferenceSection>
    );
};
