import { DeleteOutlined } from '@ant-design/icons';
import { Dropdown, Modal, message } from 'antd';
import React from 'react';

import { MenuIcon } from '@app/entity/shared/EntityDropdown/EntityDropdown';
import { MenuItemStyle } from '@app/entity/view/menu/item/styledComponent';

import { useDeleteBusinessAttributeMutation } from '@graphql/businessAttribute.generated';

type Props = {
    urn: string;
    title: string | undefined;
    onDelete?: () => void;
};

export default function BusinessAttributeItemMenu({ title, urn, onDelete }: Props) {
    const [deleteBusinessAttributeMutation] = useDeleteBusinessAttributeMutation();

    const deletePost = () => {
        deleteBusinessAttributeMutation({
            variables: {
                urn,
            },
        })
            .then(({ errors }) => {
                if (!errors) {
                    message.success('Deleted Business Attribute!');
                    onDelete?.();
                }
            })
            .catch(() => {
                message.destroy();
                message.error({
                    content: `Failed to delete Business Attribute!: An unknown error occurred.`,
                    duration: 3,
                });
            });
    };

    const onConfirmDelete = () => {
        Modal.confirm({
            title: `Delete Business Attribute '${title}'`,
            content: `Are you sure you want to remove this Business Attribute?`,
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
    ];

    return (
        <Dropdown trigger={['click']} menu={{ items }}>
            <MenuIcon data-testid={`dropdown-menu-${urn}`} fontSize={20} />
        </Dropdown>
    );
}
