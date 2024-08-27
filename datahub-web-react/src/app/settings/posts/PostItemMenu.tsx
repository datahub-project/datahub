import React from 'react';
import { DeleteOutlined, EditOutlined } from '@ant-design/icons';
import { Dropdown, Menu, message, Modal } from 'antd';
import { useTranslation } from 'react-i18next';
import { MenuIcon } from '../../entity/shared/EntityDropdown/EntityDropdown';
import { useDeletePostMutation } from '../../../graphql/post.generated';
import handleGraphQLError from '../../shared/handleGraphQLError';

type Props = {
    urn: string;
    title: string;
    onDelete?: () => void;
    onEdit?: () => void;
};

export default function PostItemMenu({ title, urn, onDelete, onEdit }: Props) {

    const { t } = useTranslation();
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
            title: t('crud.deletePost', {title}),
            content: t('post.removePostDescription'),
            onOk() {
                deletePost();
            },
            onCancel() {},
            okText: t('common.yes'),
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
                        <DeleteOutlined /> &nbsp;{t('crud.delete')}
                    </Menu.Item>
                    <Menu.Item onClick={onEdit} key="edit">
                        <EditOutlined /> &nbsp;{t('common.edit')}
                    </Menu.Item>
                </Menu>
            }
        >
            <MenuIcon data-testid={`dropdown-menu-${urn}`} fontSize={20} />
        </Dropdown>
    );
}
