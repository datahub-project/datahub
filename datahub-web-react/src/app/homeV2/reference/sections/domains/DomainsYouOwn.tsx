import React, { useState } from 'react';

import { useUserContext } from '@app/context/useUserContext';
import { EmbeddedListSearchModal } from '@app/entityV2/shared/components/styled/search/EmbeddedListSearchModal';
import { DomainMiniPreview } from '@app/entityV2/shared/links/DomainMiniPreview';
import { ReferenceSection } from '@app/homeV2/layout/shared/styledComponents';
import { EntityLinkList } from '@app/homeV2/reference/sections/EntityLinkList';
import { EmptyDomainsYouOwn } from '@app/homeV2/reference/sections/domains/EmptyDomainsYouOwn';
import { useGetDomainsYouOwn } from '@app/homeV2/reference/sections/domains/useGetDomainsYouOwn';
import { ReferenceSectionProps } from '@app/homeV2/reference/types';
import { ENTITY_FILTER_NAME, OWNERS_FILTER_NAME, UnionType } from '@app/searchV2/utils/constants';

import { Domain, EntityType } from '@types';

const DEFAULT_MAX_ENTITIES_TO_SHOW = 5;

export const DomainsYouOwn = ({ hideIfEmpty, trackClickInSection }: ReferenceSectionProps) => {
    const userContext = useUserContext();
    const { user } = userContext;
    const [entityCount, setEntityCount] = useState(DEFAULT_MAX_ENTITIES_TO_SHOW);
    const [showModal, setShowModal] = useState(false);
    const { entities, loading } = useGetDomainsYouOwn(user);

    if (hideIfEmpty && entities.length === 0) {
        return null;
    }

    return (
        <ReferenceSection>
            <EntityLinkList
                loading={loading || !user}
                entities={entities.slice(0, entityCount)}
                title="Your domains"
                tip="Domains that you are an owner of"
                showMore={entities.length > entityCount}
                showMoreCount={
                    entityCount + DEFAULT_MAX_ENTITIES_TO_SHOW > entities.length
                        ? entities.length - entityCount
                        : DEFAULT_MAX_ENTITIES_TO_SHOW
                }
                onClickMore={() => setEntityCount(entityCount + DEFAULT_MAX_ENTITIES_TO_SHOW)}
                onClickTitle={() => setShowModal(true)}
                empty={<EmptyDomainsYouOwn />}
                render={(entity) => <DomainMiniPreview domain={entity as Domain} />}
                onClickEntity={trackClickInSection}
            />
            {showModal && (
                <EmbeddedListSearchModal
                    title="Your domains"
                    fixedFilters={{
                        unionType: UnionType.AND,
                        filters: [
                            { field: OWNERS_FILTER_NAME, values: [user?.urn as string] },
                            { field: ENTITY_FILTER_NAME, values: [EntityType.Domain] },
                        ],
                    }}
                    onClose={() => setShowModal(false)}
                    placeholderText="Filter domains you own..."
                />
            )}
        </ReferenceSection>
    );
};
