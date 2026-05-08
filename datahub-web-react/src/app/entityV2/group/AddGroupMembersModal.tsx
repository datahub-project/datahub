import { Modal } from '@components';
import { message } from 'antd';
import React, { useCallback, useState } from 'react';

import { ActorsSearchSelect } from '@app/entityV2/shared/EntitySearchSelect/ActorsSearchSelect';
import { ActorEntity } from '@app/entityV2/shared/utils/actorUtils';
import { getModalDomContainer } from '@src/utils/focus';

import { useAddGroupMembersMutation } from '@graphql/group.generated';
import { EntityType } from '@types';

type Props = {
    urn: string;
    visible: boolean;
    onCloseModal: () => void;
    onSubmit: () => void;
};

export const AddGroupMembersModal = ({ urn, visible, onCloseModal, onSubmit }: Props) => {
    const [selectedMemberUrns, setSelectedMemberUrns] = useState<string[]>([]);
    const [addGroupMembersMutation] = useAddGroupMembersMutation();

    const handleActorsUpdate = useCallback((actors: ActorEntity[]) => {
        setSelectedMemberUrns(actors.map((a) => a.urn));
    }, []);

    const onModalClose = () => {
        setSelectedMemberUrns([]);
        onCloseModal();
    };

    const onAdd = async () => {
        if (selectedMemberUrns.length === 0) {
            return;
        }
        try {
            await addGroupMembersMutation({
                variables: {
                    groupUrn: urn,
                    userUrns: selectedMemberUrns,
                },
            });
            message.success({ content: 'Group members added!', duration: 3 });
        } catch (e: unknown) {
            message.destroy();
            if (e instanceof Error) {
                message.error({ content: `Failed to group members: \n ${e.message || ''}`, duration: 3 });
            }
        } finally {
            onSubmit();
            onModalClose();
        }
    };

    return (
        <Modal
            title="Add group members"
            open={visible}
            onCancel={onModalClose}
            buttons={[
                {
                    text: 'Cancel',
                    variant: 'text',
                    onClick: onModalClose,
                },
                {
                    text: 'Add',
                    onClick: onAdd,
                    variant: 'filled',
                    disabled: selectedMemberUrns.length === 0,
                    buttonDataTestId: 'modal-add-member-button',
                },
            ]}
            getContainer={getModalDomContainer}
        >
            <ActorsSearchSelect
                selectedActorUrns={selectedMemberUrns}
                onUpdate={handleActorsUpdate}
                placeholder="Search for users..."
                entityTypes={[EntityType.CorpUser]}
                width="full"
                dataTestId="add-members-select"
            />
        </Modal>
    );
};
