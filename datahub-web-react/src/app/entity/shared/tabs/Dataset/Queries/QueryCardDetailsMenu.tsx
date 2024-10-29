import React from 'react';
import styled from 'styled-components';
import { DeleteOutlined, MoreOutlined } from '@ant-design/icons';
import { Dropdown, message, Modal } from 'antd';
import { useDeleteQueryMutation } from '../../../../../../graphql/query.generated';
import handleGraphQLError from '../../../../../shared/handleGraphQLError';
import { MenuItemStyle } from '../../../../view/menu/item/styledComponent';

const StyledMoreOutlined = styled(MoreOutlined)`
    font-size: 14px;
`;

export type Props = {
    urn: string;
    onDeleted?: (urn: string) => void;
    index?: number;
};

export default function QueryCardDetailsMenu({ urn, onDeleted, index }: Props) {
    const [deleteQueryMutation] = useDeleteQueryMutation();

    const deleteQuery = () => {
        deleteQueryMutation({ variables: { urn } })
            .then(({ errors }) => {
                if (!errors) {
                    message.success({
                        content: `Deleted Query!`,
                        duration: 3,
                    });
                    onDeleted?.(urn);
                }
            })
            .catch((error) => {
                handleGraphQLError({
                    error,
                    defaultMessage: 'Failed to delete Query! An unexpected error occurred',
                    permissionMessage: 'Unauthorized to delete Query. Please contact your DataHub administrator.',
                });
            });
    };

    const confirmDeleteQuery = () => {
        Modal.confirm({
            title: `Delete Query`,
            content: `Are you sure you want to delete this query?`,
            onOk() {
                deleteQuery();
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
                <MenuItemStyle onClick={confirmDeleteQuery} data-testid={`query-delete-button-${index}`}>
                    <DeleteOutlined /> &nbsp; Delete
                </MenuItemStyle>
            ),
        },
    ];

    return (
        <Dropdown menu={{ items }} trigger={['click']}>
            <StyledMoreOutlined data-testid={`query-more-button-${index}`} />
        </Dropdown>
    );
}
