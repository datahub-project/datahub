import React, { useState } from 'react';
import { useHistory } from 'react-router';

import { useUserContext } from '@app/context/useUserContext';
import { ReferenceSection } from '@app/homeV2/layout/shared/styledComponents';
import { EntityLinkList } from '@app/homeV2/reference/sections/EntityLinkList';
import { EmptyAssetsYouSubscribeTo } from '@app/homeV2/reference/sections/subscriptions/EmptyAssetsYouSubscribeTo';
import { useGetAssetsYouSubscribeTo } from '@app/homeV2/reference/sections/subscriptions/useGetAssetsYouSubscribeTo';
import { ReferenceSectionProps } from '@app/homeV2/reference/types';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { EntityType } from '@types';

const DEFAULT_MAX_ENTITIES_TO_SHOW = 5;

// TODO: Add group ownership into this.
export const AssetsYouSubscribeTo = ({ hideIfEmpty, trackClickInSection }: ReferenceSectionProps) => {
    const history = useHistory();
    const entityRegistry = useEntityRegistry();
    const userContext = useUserContext();
    const { user } = userContext;
    const [entityCount, setEntityCount] = useState(DEFAULT_MAX_ENTITIES_TO_SHOW);
    const { entities, loading } = useGetAssetsYouSubscribeTo(user);

    if (hideIfEmpty && entities.length === 0) {
        return null;
    }

    const navigateToUserSubscriptionsTab = () => {
        history.push(`${entityRegistry.getEntityUrl(EntityType.CorpUser, user?.urn as string)}/subscriptions`);
    };

    return (
        <ReferenceSection>
            <EntityLinkList
                loading={loading || !user}
                entities={entities.slice(0, entityCount)}
                title="Your subscriptions"
                tip="Things you are subscribed to"
                showMore={entities.length > entityCount}
                showMoreCount={
                    entityCount + DEFAULT_MAX_ENTITIES_TO_SHOW > entities.length
                        ? entities.length - entityCount
                        : DEFAULT_MAX_ENTITIES_TO_SHOW
                }
                onClickMore={() => setEntityCount(entityCount + DEFAULT_MAX_ENTITIES_TO_SHOW)}
                onClickTitle={navigateToUserSubscriptionsTab}
                empty={<EmptyAssetsYouSubscribeTo />}
                onClickEntity={trackClickInSection}
            />
        </ReferenceSection>
    );
};
