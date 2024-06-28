import React from 'react';
import { DeleteOutlined } from '@ant-design/icons';
import { Dropdown, Menu, message, Modal } from 'antd';
import { MenuIcon } from '../entity/shared/EntityDropdown/EntityDropdown';
import { useDeleteBusinessAttributeMutation } from '../../graphql/businessAttribute.generated';

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

    return (
        <Dropdown
            trigger={['click']}
            overlay={
                <Menu>
                    <Menu.Item onClick={onConfirmDelete} key="delete">
                        <DeleteOutlined /> &nbsp;Delete
                    </Menu.Item>
                </Menu>
            }
        >
            <MenuIcon data-testid={`dropdown-menu-${urn}`} fontSize={20} />
        </Dropdown>
    );
}
