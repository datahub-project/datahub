import React from 'react';
import { DeleteOutlined } from '@ant-design/icons';
import { Dropdown, Menu, message, Modal } from 'antd';
import { EntityType } from '../../types.generated';
import { useEntityRegistry } from '../useEntityRegistry';
import { useDeleteDomainMutation } from '../../graphql/domain.generated';
import { MenuIcon } from '../entity/shared/EntityDropdown/EntityDropdown';

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
                    message.success('成功删除 Domain!');
                    onDelete?.();
                }
            })
            .catch(() => {
                message.destroy();
                message.error({ content: `删除 Domain 失败!: ❌ 未知错误.`, duration: 3 });
            });
    };

    const onConfirmDelete = () => {
        Modal.confirm({
            title: `删除 Domain '${name}'`,
            content: `您确定要删除 ${entityRegistry.getEntityName(EntityType.Domain)}?`,
            onOk() {
                deleteDomain();
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
