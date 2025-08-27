import { Popconfirm, message } from 'antd';
import React from 'react';

import { useDeleteRoleMutation } from '@graphql/mutations.generated';
import { DataHubRole } from '@types';

type Props = {
    open: boolean;
    role: DataHubRole | null;
    onClose: () => void;
    onConfirm: () => void;
};

/**
 * Confirmation popover for role deletion following DataHub's standard pattern
 */
export default function DeleteRoleConfirmation({ open, role, onClose, onConfirm }: Props) {
    const [deleteRole, { loading }] = useDeleteRoleMutation();

    const handleDelete = async () => {
        if (!role) return;

        try {
            await deleteRole({
                variables: { urn: role.urn },
            });

            message.success(`Successfully deleted role "${role.name}"`);
            onConfirm();
            onClose();
        } catch (error) {
            message.error(`Failed to delete role "${role.name}". Please try again.`);
        }
    };

    let title: string;
    if (role?.description) {
        title = `Delete role "${role.name}"? This will permanently delete "${role.name}" (${role.description}) and remove all associated permissions from users.`;
    } else if (role) {
        title = `Delete role "${role.name}"? This will permanently delete "${role.name}" and remove all associated permissions from users.`;
    } else {
        title = 'Delete role? This will permanently delete the role and remove all associated permissions from users.';
    }

    return (
        <Popconfirm
            title={title}
            open={open}
            onConfirm={handleDelete}
            onCancel={onClose}
            okText="Delete"
            cancelText="Cancel"
            okButtonProps={{ danger: true, loading }}
            placement="topRight"
        />
    );
}
