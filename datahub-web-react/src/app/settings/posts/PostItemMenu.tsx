import { DeleteOutlined, EditOutlined } from '@ant-design/icons';
import { Dropdown, Modal, message } from 'antd';
import React from 'react';

import { MenuIcon } from '@app/entity/shared/EntityDropdown/EntityDropdown';
import { MenuItemStyle } from '@app/entity/view/menu/item/styledComponent';
import handleGraphQLError from '@app/shared/handleGraphQLError';

import { useDeletePostMutation } from '@graphql/post.generated';

type Props = {
    urn: string;
    title: string;
    onDelete?: () => void;
    onEdit?: () => void;
};

export default function PostItemMenu({ title, urn, onDelete, onEdit }: Props) {
    const [deletePostMutation] = useDeletePostMutation();

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

    const onConfirmDelete = () => {
        Modal.confirm({
            title: `Delete Post '${title}'`,
            content: `Are you sure you want to remove this Post?`,
            onOk() {
                deletePost();
            },
            onCancel() {},
            okText: 'Yes',
            maskClosable: true,
            closable: true,
        });
    };

    const items = [
        {
            key: 'delete',
            label: (
                <MenuItemStyle onClick={onConfirmDelete}>
                    <DeleteOutlined /> &nbsp;Delete
                </MenuItemStyle>
            ),
        },
        {
            key: 'edit',
            label: (
                <MenuItemStyle onClick={onEdit}>
                    <EditOutlined /> &nbsp;Edit
                </MenuItemStyle>
            ),
        },
    ];

    return (
        <Dropdown trigger={['click']} menu={{ items }}>
            <MenuIcon data-testid={`dropdown-menu-${title}`} fontSize={20} />
        </Dropdown>
    );
}
