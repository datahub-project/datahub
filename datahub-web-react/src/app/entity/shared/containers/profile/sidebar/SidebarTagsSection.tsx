import React from 'react';
import styled from 'styled-components';

import TagTermGroup from '../../../../../shared/tags/TagTermGroup';
import { SidebarHeader } from './SidebarHeader';
import { useEntityData, useRefetch } from '../../../EntityContext';

const TermSection = styled.div`
    margin-top: 20px;
`;

export const SidebarTagsSection = ({ properties }: { properties?: any }) => {
    const canAddTag = properties?.hasTags;
    const canAddTerm = properties?.hasTerms;

    const { urn, entityType, entityData } = useEntityData();

    const showTermGroup = (entityData?.glossaryTerms?.terms?.length || 0) > 0;
    const refetch = useRefetch();
    return (
        <div>
            <SidebarHeader title={showTermGroup ? 'Tags' : 'Tags & Terms'} />
            <TagTermGroup
                editableTags={entityData?.globalTags}
                canAddTag={canAddTag}
                canAddTerm={!showTermGroup}
                canRemove
                showEmptyMessage
                entityUrn={urn}
                entityType={entityType}
                refetch={refetch}
            />
            {showTermGroup && (
                <TermSection>
                    <SidebarHeader title="Glossary Terms" />
                    <TagTermGroup
                        editableGlossaryTerms={entityData?.glossaryTerms}
                        canAddTerm={canAddTerm}
                        canRemove
                        showEmptyMessage
                        entityUrn={urn}
                        entityType={entityType}
                        refetch={refetch}
                    />
                </TermSection>
            )}
        </div>
    );
};
