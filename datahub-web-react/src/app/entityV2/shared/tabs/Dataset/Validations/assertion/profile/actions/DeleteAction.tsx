import { DeleteOutlined } from '@ant-design/icons';
import { Modal, message } from 'antd';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { useDeleteAssertionMutationWithCache } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/hooks';
import { ActionItem } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/actions/ActionItem';

import { Assertion } from '@types';

const StyledDeleteOutlined = styled(DeleteOutlined)`
    && {
        font-size: 12px;
        display: flex;
    }
`;

type Props = {
    assertion: Assertion;
    canEdit: boolean;
    refetch?: () => void;
    isExpandedView?: boolean;
};

export const DeleteAction = ({ assertion, canEdit, refetch, isExpandedView = false }: Props) => {
    const { t } = useTranslation('entity.profile.validations');
    const { t: tc } = useTranslation('common.actions');
    const [deleteAssertionMutation] = useDeleteAssertionMutationWithCache();

    const deleteAssertion = async () => {
        try {
            const response = await deleteAssertionMutation({ variables: { urn: assertion.urn } });
            if (!response.data?.deleteAssertion) {
                throw new Error('Assertion deletion was not acknowledged');
            }
            await message.success({ content: t('action.removedAssertion'), duration: 2 });
            refetch?.();
        } catch {
            message.destroy();
            message.error({ content: t('action.failedRemoveAssertion'), duration: 3 });
        }
    };

    const onDeleteAssertion = () => {
        Modal.confirm({
            title: t('action.confirmRemovalTitle'),
            content: t('action.confirmRemovalContent'),
            onOk: deleteAssertion,
            okText: tc('yes'),
            maskClosable: true,
            closable: true,
        });
    };

    return (
        <ActionItem
            key="delete"
            tip={canEdit ? t('action.deleteAssertionTip') : t('action.noPermissionDelete')}
            disabled={!canEdit}
            onClick={onDeleteAssertion}
            icon={<StyledDeleteOutlined />}
            isExpandedView={isExpandedView}
            actionName={tc('delete')}
        />
    );
};
