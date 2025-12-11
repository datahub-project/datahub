import AddRoundedIcon from '@mui/icons-material/AddRounded';
import React, { useState } from 'react';
import styled from 'styled-components';

import { useEntityData, useMutationUrn, useRefetch } from '@app/entity/shared/EntityContext';
import { EMPTY_MESSAGES } from '@app/entityV2/shared/constants';
import EmptySectionText from '@app/entityV2/shared/containers/profile/sidebar/EmptySectionText';
import SectionActionButton from '@app/entityV2/shared/containers/profile/sidebar/SectionActionButton';
import { SidebarSection } from '@app/entityV2/shared/containers/profile/sidebar/SidebarSection';
import { useEntityDataExtractor } from '@app/entityV2/shared/containers/profile/sidebar/hooks/useEntityDataExtractor';
import { getProposedItemsByType } from '@app/entityV2/shared/utils';
import { ENTITY_PROFILE_TAGS_ID } from '@app/onboarding/config/EntityProfileOnboardingConfig';
import { findTopLevelProposals } from '@app/shared/tags/utils/proposalUtils';
import AddTagTerm from '@app/sharedV2/tags/AddTagTerm';
import TagTermGroup from '@app/sharedV2/tags/TagTermGroup';

import { ActionRequestType, EntityType } from '@types';

const Content = styled.div`
    display: flex;
    align-items: start;
    justify-content: start;
    flex-wrap: wrap;
`;

interface Props {
    readOnly?: boolean;
    properties?: any;
}

export const SidebarTagsSection = ({ readOnly, properties }: Props) => {
    const { entityType, entityData } = useEntityData();
    const refetch = useRefetch();
    const mutationUrn = useMutationUrn();

    const [showAddModal, setShowAddModal] = useState(false);
    const [addModalType, setAddModalType] = useState<EntityType | undefined>(undefined);

    // Extract tags using custom hook (for Business Attributes support)
    const { data: tags, isEmpty: areTagsEmpty } = useEntityDataExtractor({
        customPath: properties?.customTagPath,
        defaultPath: 'globalTags',
        arrayProperty: 'tags',
    });

    // Preserve Acryl proposals functionality
    const proposedTags = findTopLevelProposals(
        getProposedItemsByType(entityData?.proposals || [], ActionRequestType.TagAssociation) || [],
    );

    const areTagsEmptyWithProposals = areTagsEmpty && !proposedTags?.length;

    const canEditTags = !!entityData?.privileges?.canEditTags;
    const canProposeTags = !!entityData?.privileges?.canProposeTags;

    return (
        <div id={ENTITY_PROFILE_TAGS_ID}>
            <SidebarSection
                title="Tags"
                content={
                    <Content>
                        {!areTagsEmptyWithProposals ? (
                            <TagTermGroup
                                editableTags={tags}
                                canAddTag
                                canRemove
                                showEmptyMessage
                                entityUrn={mutationUrn}
                                entityType={entityType}
                                refetch={refetch}
                                readOnly={readOnly}
                                fontSize={12}
                                proposedTags={proposedTags}
                                showAddButton={false}
                            />
                        ) : (
                            <EmptySectionText message={EMPTY_MESSAGES.tags.title} />
                        )}
                    </Content>
                }
                extra={
                    <SectionActionButton
                        button={<AddRoundedIcon />}
                        onClick={(event) => {
                            setShowAddModal(true);
                            setAddModalType(EntityType.Tag);
                            event.stopPropagation();
                        }}
                        actionPrivilege={canEditTags || canProposeTags}
                        dataTestId="add-tags-button"
                    />
                }
            />
            <AddTagTerm
                entityUrn={mutationUrn}
                entityType={entityType}
                showAddModal={showAddModal}
                setShowAddModal={setShowAddModal}
                addModalType={addModalType}
                refetch={refetch}
                canAddTag={canEditTags}
                canProposeTag={canProposeTags}
            />
        </div>
    );
};
