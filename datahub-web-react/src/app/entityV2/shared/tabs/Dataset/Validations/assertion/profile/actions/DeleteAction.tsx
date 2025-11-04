import { DeleteOutlined } from '@ant-design/icons';
import { Modal, message } from 'antd';
import React from 'react';
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
    // Should be defined if canEdit
    refetch?: () => void;
    isExpandedView?: boolean;
};

export const DeleteAction = ({ assertion, canEdit, refetch, isExpandedView = false }: Props) => {
    const [deleteAssertionMutation] = useDeleteAssertionMutationWithCache();

    const deleteAssertion = async () => {
        const assertionUrn = assertion.urn;
        try {
            await deleteAssertionMutation({
                variables: { urn: assertionUrn as string },
            });
            await message.success({ content: 'Removed assertion.', duration: 2 });
        } catch (e: unknown) {
            message.destroy();
            if (e instanceof Error) {
                message.error({ content: `Failed to remove assertion. An unknown error occurred`, duration: 3 });
            }
        }
        refetch?.();
    };

    const onDeleteAssertion = () => {
        Modal.confirm({
            title: `Confirm Assertion Removal`,
            content: `Are you sure you want to remove this assertion from the dataset?`,
            onOk() {
                deleteAssertion();
            },
            onCancel() {},
            okText: 'Yes',
            maskClosable: true,
            closable: true,
        });
    };

    const authorizedTip = 'Delete this assertion';
    const unauthorizedTip = 'You do not have permission to delete this assertion';

    return (
        <ActionItem
            key="delete"
            tip={canEdit ? authorizedTip : unauthorizedTip}
            disabled={!canEdit}
            onClick={onDeleteAssertion}
            icon={<StyledDeleteOutlined />}
            isExpandedView={isExpandedView}
            actionName="Delete"
        />
    );
};
