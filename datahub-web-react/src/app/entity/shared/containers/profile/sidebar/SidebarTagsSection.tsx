import React from 'react';
import analytics, { EntityActionType, EventType } from '../../../../../analytics';
import TagTermGroup from '../../../../../shared/tags/TagTermGroup';
import { GenericEntityUpdate } from '../../../types';
import { SidebarHeader } from './SidebarHeader';
import { useEntityData, useEntityUpdate } from '../../../EntityContext';

export const SidebarTagsSection = () => {
    const { urn, entityType, entityData } = useEntityData();
    const updateEntity = useEntityUpdate<GenericEntityUpdate>();
    return (
        <div>
            <SidebarHeader title="Tags" />
            <TagTermGroup
                editableTags={entityData?.globalTags}
                glossaryTerms={entityData?.glossaryTerms}
                canAdd
                canRemove
                showEmptyMessage
                updateTags={(globalTags) => {
                    analytics.event({
                        type: EventType.EntityActionEvent,
                        actionType: EntityActionType.UpdateTags,
                        entityType,
                        entityUrn: urn,
                    });
                    return updateEntity({ variables: { input: { urn, globalTags } } });
                }}
            />
        </div>
    );
};
