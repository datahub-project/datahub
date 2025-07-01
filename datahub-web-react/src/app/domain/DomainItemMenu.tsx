import { DeleteOutlined } from '@ant-design/icons';
import { Dropdown, Menu, Modal, message } from 'antd';
import React from 'react';

import { MenuIcon } from '@app/entity/shared/EntityDropdown/EntityDropdown';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { useDeleteDomainMutation } from '@graphql/domain.generated';
import { EntityType } from '@types';

type Props = {
    urn: string;
    name: string;
    onDelete?: () => void;
};

export default function DomainItemMenu({ name, urn, onDelete }: Props) {
    const entityRegistry = useEntityRegistry();
    const [deleteDomainMutation] = useDeleteDomainMutation();

    const deleteDomain = () => {
        deleteDomainMutation({
            variables: {
                urn,
            },
        })
            .then(({ errors }) => {
                if (!errors) {
                    message.success('Deleted Domain!');
                    onDelete?.();
                }
            })
            .catch(() => {
                message.destroy();
                message.error({ content: `Failed to delete Domain!: An unknown error occurred.`, duration: 3 });
            });
    };

    const onConfirmDelete = () => {
        Modal.confirm({
            title: `Delete Domain '${name}'`,
            content: `Are you sure you want to remove this ${entityRegistry.getEntityName(EntityType.Domain)}?`,
            onOk() {
                deleteDomain();
            },
            onCancel() {},
            okText: 'Yes',
            maskClosable: true,
            closable: true,
        });
    };

    const items = [
        {
            key: 0,
            label: (
                <Menu.Item onClick={onConfirmDelete} key="delete">
                    <DeleteOutlined /> &nbsp;Delete
                </Menu.Item>
            ),
        },
    ];

    return (
        <Dropdown trigger={['click']} menu={{ items }}>
            <MenuIcon data-testid={`dropdown-menu-${urn}`} fontSize={20} />
        </Dropdown>
    );
}
