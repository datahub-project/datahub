import { Button, Form, Modal, message } from 'antd';
import React, { useCallback, useState } from 'react';
import { useTranslation } from 'react-i18next';

import { ActorsSearchSelect } from '@app/entityV2/shared/EntitySearchSelect/ActorsSearchSelect';
import { ActorEntity } from '@app/entityV2/shared/utils/actorUtils';

import { useAddGroupMembersMutation } from '@graphql/group.generated';
import { EntityType } from '@types';

type Props = {
    urn: string;
    open: boolean;
    onCloseModal: () => void;
    onSubmit: () => void;
};

export const AddGroupMembersModal = ({ urn, open, onCloseModal, onSubmit }: Props) => {
    const { t } = useTranslation('entity.identity');
    const { t: tc } = useTranslation('common.actions');
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
            message.success({ content: t('groups.addMembersSuccess'), duration: 3 });
        } catch (e: unknown) {
            message.destroy();
            if (e instanceof Error) {
                message.error({ content: t('groups.addMembersFailure', { error: e.message || '' }), duration: 3 });
            }
        } finally {
            onSubmit();
            onModalClose();
        }
    };

    return (
        <Modal
            title={t('groups.addMembersModal.title')}
            open={open}
            onCancel={onModalClose}
            footer={
                <>
                    <Button onClick={onModalClose} type="text">
                        {tc('cancel')}
                    </Button>
                    <Button disabled={selectedMemberUrns.length === 0} onClick={onAdd}>
                        {tc('add')}
                    </Button>
                </>
            }
        >
            <Form component={false}>
                <Form.Item>
                    <ActorsSearchSelect
                        selectedActorUrns={selectedMemberUrns}
                        onUpdate={handleActorsUpdate}
                        placeholder={t('groups.addMembersModal.searchPlaceholder')}
                        entityTypes={[EntityType.CorpUser]}
                        width="full"
                        dataTestId="add-members-select"
                    />
                </Form.Item>
            </Form>
        </Modal>
    );
};
