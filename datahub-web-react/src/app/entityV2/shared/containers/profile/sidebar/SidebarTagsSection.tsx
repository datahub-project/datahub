import AddRoundedIcon from '@mui/icons-material/AddRounded';
import React, { useState } from 'react';
import styled from 'styled-components';

import { useEntityData, useMutationUrn, useRefetch } from '@app/entity/shared/EntityContext';
import { EMPTY_MESSAGES } from '@app/entityV2/shared/constants';
import EmptySectionText from '@app/entityV2/shared/containers/profile/sidebar/EmptySectionText';
import SectionActionButton from '@app/entityV2/shared/containers/profile/sidebar/SectionActionButton';
import { SidebarSection } from '@app/entityV2/shared/containers/profile/sidebar/SidebarSection';
import { ENTITY_PROFILE_TAGS_ID } from '@app/onboarding/config/EntityProfileOnboardingConfig';
import AddTagTerm from '@app/sharedV2/tags/AddTagTerm';
import TagTermGroup from '@app/sharedV2/tags/TagTermGroup';

import { EntityType } from '@types';

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

    const [showAddModal, setShowAddModal] = useState(false);
    const [addModalType, setAddModalType] = useState<EntityType | undefined>(undefined);

    const areTagsEmpty = !entityData?.globalTags?.tags?.length;

    const canEditTags = !!entityData?.privileges?.canEditTags;

    return (
        <div id={ENTITY_PROFILE_TAGS_ID}>
            <SidebarSection
                title="Tags"
                content={
                    <Content>
                        {!areTagsEmpty ? (
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
                        actionPrivilege={canEditTags}
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
            />
        </div>
    );
};
