import React, { useState } from 'react';
import { useUserContext } from '../../../../context/useUserContext';
import { EntityLinkList } from '../EntityLinkList';
import { EmbeddedListSearchModal } from '../../../../entityV2/shared/components/styled/search/EmbeddedListSearchModal';
import { ENTITY_FILTER_NAME, OWNERS_FILTER_NAME, UnionType } from '../../../../searchV2/utils/constants';
import { useGetTagsYouOwn } from './useGetTagsYouOwn';
import { EmptyTagsYouOwn } from './EmptyTagsYouOwn';
import { EntityType } from '../../../../../types.generated';
import TagLink from '../../../../sharedV2/tags/TagLink';
import { ReferenceSectionProps } from '../../types';
import { ReferenceSection } from '../../../layout/shared/styledComponents';

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
