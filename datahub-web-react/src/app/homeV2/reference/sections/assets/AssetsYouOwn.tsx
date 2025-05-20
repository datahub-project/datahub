import React, { useState } from 'react';

import { useUserContext } from '@app/context/useUserContext';
import { EmbeddedListSearchModal } from '@app/entityV2/shared/components/styled/search/EmbeddedListSearchModal';
import { ReferenceSection } from '@app/homeV2/layout/shared/styledComponents';
import { EntityLinkList } from '@app/homeV2/reference/sections/EntityLinkList';
import { EmptyAssetsYouOwn } from '@app/homeV2/reference/sections/assets/EmptyAssetsYouOwn';
import { useGetAssetsYouOwn } from '@app/homeV2/reference/sections/assets/useGetAssetsYouOwn';
import { ReferenceSectionProps } from '@app/homeV2/reference/types';
import { OWNERS_FILTER_NAME, UnionType } from '@app/searchV2/utils/constants';
import useGetUserGroupUrns from '@src/app/entityV2/user/useGetUserGroupUrns';

const DEFAULT_MAX_ENTITIES_TO_SHOW = 5;

// TODO: Add group ownership into this.
export const AssetsYouOwn = ({ hideIfEmpty, trackClickInSection }: ReferenceSectionProps) => {
    const userContext = useUserContext();
    const { user } = userContext;
    const [entityCount, setEntityCount] = useState(DEFAULT_MAX_ENTITIES_TO_SHOW);
    const [showModal, setShowModal] = useState(false);
    const { groupUrns } = useGetUserGroupUrns(user?.urn);
    const { entities, loading } = useGetAssetsYouOwn(user);

    if (hideIfEmpty && entities.length === 0) {
        return null;
    }

    return (
        <ReferenceSection>
            <EntityLinkList
                loading={loading || !user}
                entities={entities.slice(0, entityCount)}
                title="Your assets"
                tip="Things you are an owner of"
                showMore={entities.length > entityCount}
                showMoreCount={
                    entityCount + DEFAULT_MAX_ENTITIES_TO_SHOW > entities.length
                        ? entities.length - entityCount
                        : DEFAULT_MAX_ENTITIES_TO_SHOW
                }
                onClickMore={() => setEntityCount(entityCount + DEFAULT_MAX_ENTITIES_TO_SHOW)}
                onClickTitle={() => setShowModal(true)}
                empty={<EmptyAssetsYouOwn />}
                onClickEntity={trackClickInSection}
            />
            {showModal && (
                <EmbeddedListSearchModal
                    title="Your assets"
                    fixedFilters={{
                        unionType: UnionType.AND,
                        filters: [{ field: OWNERS_FILTER_NAME, values: [user?.urn as string, ...groupUrns] }],
                    }}
                    onClose={() => setShowModal(false)}
                    placeholderText="Filter assets you own..."
                />
            )}
        </ReferenceSection>
    );
};
