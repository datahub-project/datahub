import { DeleteOutlined, EditOutlined } from '@ant-design/icons';
import { Dropdown, Menu, message } from 'antd';
import React, { useState } from 'react';

import { MenuIcon } from '@app/entity/shared/EntityDropdown/EntityDropdown';
import handleGraphQLError from '@app/shared/handleGraphQLError';
import { ConfirmationModal } from '@app/sharedV2/modals/ConfirmationModal';

import { useDeletePostMutation } from '@graphql/post.generated';

type Props = {
    urn: string;
    title: string;
    onDelete?: () => void;
    onEdit?: () => void;
};

export default function PostItemMenu({ title, urn, onDelete, onEdit }: Props) {
    const [deletePostMutation] = useDeletePostMutation();
    const [showConfirmDelete, setShowConfirmDelete] = useState(false);

    const deletePost = () => {
        deletePostMutation({
            variables: {
                urn,
            },
        })
            .then(({ errors }) => {
                if (!errors) {
                    message.success('Deleted Post!');
                    onDelete?.();
                }
            })
            .catch((error) => {
                handleGraphQLError({
                    error,
                    defaultMessage: 'Failed to delete Post! An unexpected error occurred',
                    permissionMessage: 'Unauthorized to delete Post. Please contact your DataHub administrator.',
                });
            });
    };

    return (
        <>
            <Dropdown
                trigger={['click']}
                overlay={
                    <Menu>
                        <Menu.Item onClick={() => setShowConfirmDelete(true)} key="delete">
                            <DeleteOutlined /> &nbsp;Delete
                        </Menu.Item>
                        <Menu.Item onClick={onEdit} key="edit">
                            <EditOutlined /> &nbsp;Edit
                        </Menu.Item>
                    </Menu>
                }
            >
                <MenuIcon data-testid="dropdown-menu-item" fontSize={20} />
            </Dropdown>
            <ConfirmationModal
                isOpen={showConfirmDelete}
                handleClose={() => setShowConfirmDelete(false)}
                handleConfirm={deletePost}
                modalTitle={`Delete Post '${title}'`}
                modalText="Are you sure you want to remove this Post?"
            />
        </>
    );
}
