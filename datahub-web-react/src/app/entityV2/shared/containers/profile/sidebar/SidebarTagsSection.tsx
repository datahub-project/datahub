import React from 'react';
import styled from 'styled-components';
import TagTermGroup from '../../../../../sharedV2/tags/TagTermGroup';
import { useEntityData, useMutationUrn, useRefetch } from '../../../EntityContext';
import { findTopLevelProposals } from '../../../../../shared/tags/utils/proposalUtils';
import { ENTITY_PROFILE_TAGS_ID } from '../../../../../onboarding/config/EntityProfileOnboardingConfig';
import { SidebarSection } from './SidebarSection';

const Content = styled.div`
    display: flex;
    align-items: start;
    justify-content: start;
    flex-wrap: wrap;
`;

interface Props {
    readOnly?: boolean;
}

export const SidebarTagsSection = ({ readOnly }: Props) => {
    const { entityType, entityData } = useEntityData();
    const refetch = useRefetch();
    const mutationUrn = useMutationUrn();

    return (
        <div id={ENTITY_PROFILE_TAGS_ID}>
            <SidebarSection
                title="Tags"
                content={
                    <Content>
                        <TagTermGroup
                            editableTags={entityData?.globalTags}
                            canAddTag
                            canRemove
                            showEmptyMessage
                            entityUrn={mutationUrn}
                            entityType={entityType}
                            refetch={refetch}
                            readOnly={readOnly}
                            fontSize={12}
                            proposedTags={findTopLevelProposals(entityData?.tagProposals || [])}
                        />
                    </Content>
                }
            />
        </div>
    );
};
