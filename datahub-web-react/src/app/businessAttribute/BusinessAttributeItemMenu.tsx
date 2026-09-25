import { DeleteOutlined } from '@ant-design/icons';
import { Dropdown, Modal, message } from 'antd';
import React from 'react';
import { useTranslation } from 'react-i18next';

import { MenuIcon } from '@app/entity/shared/EntityDropdown/EntityDropdown';
import { MenuItemStyle } from '@app/entity/view/menu/item/styledComponent';

import { useDeleteBusinessAttributeMutation } from '@graphql/businessAttribute.generated';

type Props = {
    urn: string;
    title: string | undefined;
    onDelete?: () => void;
};

export default function BusinessAttributeItemMenu({ title, urn, onDelete }: Props) {
    const { t } = useTranslation('misc');
    const { t: tc } = useTranslation('common.actions');
    const [deleteBusinessAttributeMutation] = useDeleteBusinessAttributeMutation();

    const deletePost = () => {
        deleteBusinessAttributeMutation({
            variables: {
                urn,
            },
        })
            .then(({ errors }) => {
                if (!errors) {
                    message.success(t('businessAttribute.deleteSuccess'));
                    onDelete?.();
                }
            })
            .catch(() => {
                message.destroy();
                message.error({
                    content: t('businessAttribute.deleteError'),
                    duration: 3,
                });
            });
    };

    const onConfirmDelete = () => {
        Modal.confirm({
            title: t('businessAttribute.deleteModalTitle', { title }),
            content: t('businessAttribute.deleteConfirmation'),
            onOk() {
                deletePost();
            },
            onCancel() {},
            okText: tc('yes'),
            maskClosable: true,
            closable: true,
        });
    };

    const items = [
        {
            key: 'delete',
            danger: true,
            label: (
                <MenuItemStyle onClick={onConfirmDelete}>
                    <DeleteOutlined /> &nbsp;{tc('delete')}
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
