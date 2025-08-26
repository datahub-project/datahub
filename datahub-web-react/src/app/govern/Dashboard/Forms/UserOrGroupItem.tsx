import { Tooltip } from '@components';
import React, { useContext, useState } from 'react';

import ManageFormContext from '@app/govern/Dashboard/Forms/ManageFormContext';
import {
    CardData,
    CardIcons,
    ItemDivider,
    ListItem,
    NameColumn,
    VerticalFlexBox,
} from '@app/govern/Dashboard/Forms/styledComponents';
import { Icon, Text } from '@src/alchemy-components';
import { CustomAvatar } from '@src/app/shared/avatar';
import { ConfirmationModal } from '@src/app/sharedV2/modals/ConfirmationModal';
import { useEntityRegistry } from '@src/app/useEntityRegistry';
import { CorpGroup, CorpUser, EntityType } from '@src/types.generated';

interface Props {
    userOrGroup: CorpUser | CorpGroup;
    isGroup: boolean;
}

const UserOrGroupItem = ({ userOrGroup, isGroup }: Props) => {
    const entityRegistry = useEntityRegistry();
    const { setFormValues } = useContext(ManageFormContext);

    const [showConfirmDelete, setShowConfirmDelete] = useState<boolean>(false);

    const name = entityRegistry.getDisplayName(isGroup ? EntityType.CorpGroup : EntityType.CorpUser, userOrGroup);
    const avatarUrl = userOrGroup.editableProperties?.pictureLink || undefined;

    const handleDeleteUser = () => {
        if (isGroup)
            setFormValues((prev) => ({
                ...prev,
                actors: {
                    ...prev.actors,
                    groups: prev.actors?.groups?.filter((group) => group.urn !== userOrGroup.urn),
                },
            }));
        else
            setFormValues((prev) => ({
                ...prev,
                actors: {
                    ...prev.actors,
                    users: prev.actors?.users?.filter((user) => user.urn !== userOrGroup.urn),
                },
            }));
        setShowConfirmDelete(false);
    };

    const handleDeleteClose = () => {
        setShowConfirmDelete(false);
    };

    return (
        <>
            <ListItem>
                <CardData width="60%">
                    <NameColumn>
                        <CustomAvatar size={30} name={name} photoUrl={avatarUrl} isGroup={false} />
                        <VerticalFlexBox>
                            <Text weight="medium">{name}</Text>
                            <Text color="gray" size="md">
                                {isGroup
                                    ? (userOrGroup as CorpGroup).properties?.description
                                    : userOrGroup.properties?.email}
                            </Text>
                        </VerticalFlexBox>
                    </NameColumn>
                </CardData>
                <CardData width="20%">
                    <Text color="gray"> {isGroup ? 'Group' : 'User'}</Text>
                </CardData>
                <CardIcons>
                    <Tooltip title={isGroup ? 'Remove group' : 'Remove user'} showArrow={false}>
                        <Icon icon="Delete" size="md" onClick={() => setShowConfirmDelete(true)} />
                    </Tooltip>
                </CardIcons>
            </ListItem>

            <ItemDivider />
            <ConfirmationModal
                isOpen={showConfirmDelete}
                handleClose={handleDeleteClose}
                handleConfirm={handleDeleteUser}
                modalTitle="Confirm Delete"
                modalText={`Are you sure you want to remove the ${isGroup ? 'group' : 'user'}?`}
            />
        </>
    );
};

export default UserOrGroupItem;
