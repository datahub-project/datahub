import React, { useState } from 'react';
import { useHistory } from 'react-router';
import { useUserContext } from '../../../../context/useUserContext';
import { EntityLinkList } from '../EntityLinkList';
import { useEntityRegistry } from '../../../../useEntityRegistry';
import { EntityType } from '../../../../../types.generated';
import { useGetGroupsYouAreIn } from './useGetGroupsYouAreIn';
import { EmptyGroupsYouAreIn } from './EmptyGroupsYouAreIn';
import { ReferenceSectionProps } from '../../types';
import { ReferenceSection } from '../../../layout/shared/styledComponents';

const DEFAULT_MAX_ENTITIES_TO_SHOW = 5;

export const GroupsYouAreIn = ({ hideIfEmpty }: ReferenceSectionProps) => {
    const history = useHistory();
    const entityRegistry = useEntityRegistry();
    const userContext = useUserContext();
    const { user } = userContext;
    const [entityCount, setEntityCount] = useState(DEFAULT_MAX_ENTITIES_TO_SHOW);
    const { entities, loading } = useGetGroupsYouAreIn(user);

    if (hideIfEmpty && entities.length === 0) {
        return null;
    }

    const navigateToUserGroupsTab = () => {
        history.push(`${entityRegistry.getEntityUrl(EntityType.CorpUser, user?.urn as string)}/groups`);
    };

    return (
        <ReferenceSection>
            <EntityLinkList
                loading={loading || !user}
                entities={entities.slice(0, entityCount)}
                title="Your groups"
                tip="The groups or teams you are part of"
                showMore={entities.length > entityCount}
                showMoreCount={
                    entityCount + DEFAULT_MAX_ENTITIES_TO_SHOW > entities.length
                        ? entities.length - entityCount
                        : DEFAULT_MAX_ENTITIES_TO_SHOW
                }
                onClickMore={() => setEntityCount(entityCount + DEFAULT_MAX_ENTITIES_TO_SHOW)}
                onClickTitle={navigateToUserGroupsTab}
                empty={<EmptyGroupsYouAreIn />}
            />
        </ReferenceSection>
    );
};
