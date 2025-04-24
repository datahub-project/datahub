import React, { useState } from 'react';

import { useUserContext } from '@app/context/useUserContext';
import { EmbeddedListSearchModal } from '@app/entityV2/shared/components/styled/search/EmbeddedListSearchModal';
import { ReferenceSection } from '@app/homeV2/layout/shared/styledComponents';
import { EntityLinkList } from '@app/homeV2/reference/sections/EntityLinkList';
import { EmptyTagsYouOwn } from '@app/homeV2/reference/sections/tags/EmptyTagsYouOwn';
import { useGetTagsYouOwn } from '@app/homeV2/reference/sections/tags/useGetTagsYouOwn';
import { ReferenceSectionProps } from '@app/homeV2/reference/types';
import { ENTITY_FILTER_NAME, OWNERS_FILTER_NAME, UnionType } from '@app/searchV2/utils/constants';
import TagLink from '@app/sharedV2/tags/TagLink';

import { EntityType } from '@types';

const DEFAULT_MAX_ENTITIES_TO_SHOW = 10;

// TODO: Add group ownership into this.
export const TagsYouOwn = ({ hideIfEmpty }: ReferenceSectionProps) => {
    const userContext = useUserContext();
    const { user } = userContext;
    const [entityCount, setEntityCount] = useState(DEFAULT_MAX_ENTITIES_TO_SHOW);
    const [showModal, setShowModal] = useState(false);
    const { entities, loading } = useGetTagsYouOwn(user);

    if (hideIfEmpty && entities.length === 0) {
        return null;
    }

    const renderTag = (tag: any) => {
        return <TagLink tag={tag} />;
    };

    return (
        <ReferenceSection>
            <EntityLinkList
                loading={loading || !user}
                entities={entities.slice(0, entityCount)}
                title="Your tags"
                tip="Tags that you created"
                showMore={entities.length > entityCount}
                onClickMore={() => setEntityCount(entityCount + DEFAULT_MAX_ENTITIES_TO_SHOW)}
                onClickTitle={() => setShowModal(true)}
                showMoreCount={
                    entityCount + DEFAULT_MAX_ENTITIES_TO_SHOW > entities.length
                        ? entities.length - entityCount
                        : DEFAULT_MAX_ENTITIES_TO_SHOW
                }
                empty={<EmptyTagsYouOwn />}
                render={renderTag}
            />
            {showModal && (
                <EmbeddedListSearchModal
                    title="Your tags"
                    fixedFilters={{
                        unionType: UnionType.AND,
                        filters: [
                            { field: OWNERS_FILTER_NAME, values: [user?.urn as string] },
                            { field: ENTITY_FILTER_NAME, values: [EntityType.Tag] },
                        ],
                    }}
                    onClose={() => setShowModal(false)}
                    placeholderText="Filter tags you own..."
                />
            )}
        </ReferenceSection>
    );
};
