import React, { useState } from 'react';
import styled from 'styled-components';
import AddRoundedIcon from '@mui/icons-material/AddRounded';
import TagTermGroup from '../../../../../sharedV2/tags/TagTermGroup';
import { useEntityData, useMutationUrn, useRefetch } from '../../../../../entity/shared/EntityContext';
import { ENTITY_PROFILE_TAGS_ID } from '../../../../../onboarding/config/EntityProfileOnboardingConfig';
import { SidebarSection } from './SidebarSection';
import { EntityType } from '../../../../../../types.generated';
import SectionActionButton from './SectionActionButton';
import AddTagTerm from '../../../../../sharedV2/tags/AddTagTerm';
import EmptySectionText from './EmptySectionText';
import { EMPTY_MESSAGES } from '../../../constants';

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
