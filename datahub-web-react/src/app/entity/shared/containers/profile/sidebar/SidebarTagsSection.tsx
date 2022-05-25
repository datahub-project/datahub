import React from 'react';
import styled from 'styled-components';

import TagTermGroup from '../../../../../shared/tags/TagTermGroup';
import { SidebarHeader } from './SidebarHeader';
import { useEntityData, useRefetch } from '../../../EntityContext';
import { useEntityCommonPrivileges } from '../../../EntityAuthorizationContext';

const TermSection = styled.div`
    margin-top: 20px;
`;

export const SidebarTagsSection = ({ properties }: { properties?: any }) => {
    const canAddTag = properties?.hasTags;
    const canAddTerm = properties?.hasTerms;

    const { urn, entityType, entityData } = useEntityData();
    // Privileges
    const { editTags, editGlossaryTerms } = useEntityCommonPrivileges();

    const refetch = useRefetch();

    return (
        <div>
            <SidebarHeader title="Tags" />
            <TagTermGroup
                editableTags={entityData?.globalTags}
                canAddTag={editTags && canAddTag}
                canRemove={editTags}
                showEmptyMessage
                entityUrn={urn}
                entityType={entityType}
                refetch={refetch}
            />
            <TermSection>
                <SidebarHeader title="Glossary Terms" />
                <TagTermGroup
                    editableGlossaryTerms={entityData?.glossaryTerms}
                    canAddTerm={editGlossaryTerms && canAddTerm}
                    canRemove={editGlossaryTerms}
                    showEmptyMessage
                    entityUrn={urn}
                    entityType={entityType}
                    refetch={refetch}
                />
            </TermSection>
        </div>
    );
};
