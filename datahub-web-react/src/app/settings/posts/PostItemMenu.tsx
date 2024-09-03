import React from 'react';
import { DeleteOutlined, EditOutlined } from '@ant-design/icons';
import { Dropdown, message, Modal } from 'antd';
import { useTranslation } from 'react-i18next';
import { MenuIcon } from '../../entity/shared/EntityDropdown/EntityDropdown';
import { useDeletePostMutation } from '../../../graphql/post.generated';
import handleGraphQLError from '../../shared/handleGraphQLError';
import { MenuItemStyle } from '../../entity/view/menu/item/styledComponent';

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
                    message.success(t('crud.success.deleteWithName', { name: t('common.post')}));
                    onDelete?.();
                }
            })
            .catch((error) => {
                handleGraphQLError({
                    error,
                    defaultMessage: t('crud.error.deleteWithName', { name: t('common.post')}),
                    permissionMessage: t('crud.error.unauthorizedToDeleteWithName', { name: t('common.post')}),
                });
            });
    };

    const onConfirmDelete = () => {
        Modal.confirm({
            title: `${t('crud.deletePost')} '${title}'`,
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

    const items = [
        {
            key: 'delete',
            label: (
                <MenuItemStyle onClick={onConfirmDelete}>
                    <DeleteOutlined /> &nbsp;{t('crud.delete')}
                </MenuItemStyle>
            ),
        },
        {
            key: 'edit',
            label: (
                <MenuItemStyle onClick={onEdit}>
                    <EditOutlined /> &nbsp;{t('common.edit')}
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
