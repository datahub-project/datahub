import React from 'react';
import TagTermGroup from '../../../../../shared/tags/TagTermGroup';
import { SidebarHeader } from './SidebarHeader';
import { useEntityData, useRefetch } from '../../../EntityContext';

export const SidebarTagsSection = ({ properties }: { properties?: any }) => {
    const canAddTag = properties?.hasTags;
    const canAddTerm = properties?.hasTerms;

    const { urn, entityType, entityData } = useEntityData();
    const refetch = useRefetch();
    return (
        <div>
            <SidebarHeader title="Tags" />
            <TagTermGroup
                editableTags={entityData?.globalTags}
                editableGlossaryTerms={entityData?.glossaryTerms}
                canAddTag={canAddTag}
                canAddTerm={canAddTerm}
                canRemove
                showEmptyMessage
                entityUrn={urn}
                entityType={entityType}
                refetch={refetch}
            />
        </div>
    );
};
