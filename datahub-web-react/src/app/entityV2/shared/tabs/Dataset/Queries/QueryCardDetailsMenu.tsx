import { DeleteOutlined, MoreOutlined } from '@ant-design/icons';
import { Dropdown, Menu, message } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';

import { ConfirmationModal } from '@app/sharedV2/modals/ConfirmationModal';

import { useDeleteQueryMutation } from '@graphql/query.generated';

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
    const [showConfirmationModal, setShowConfirmationModa] = useState(false);

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
            .catch(() => {
                message.destroy();
                message.error({ content: 'Failed to delete Query! An unexpected error occurred' });
            });
    };

    return (
        <>
            <Dropdown
                overlay={
                    <Menu>
                        <Menu.Item
                            key="0"
                            onClick={() => setShowConfirmationModa(true)}
                            data-testid={`query-delete-button-${index}`}
                        >
                            <DeleteOutlined /> &nbsp; Delete
                        </Menu.Item>
                    </Menu>
                }
                trigger={['click']}
            >
                <StyledMoreOutlined data-testid={`query-more-button-${index}`} />
            </Dropdown>
            <ConfirmationModal
                isOpen={showConfirmationModal}
                handleClose={() => setShowConfirmationModa(false)}
                handleConfirm={deleteQuery}
                modalTitle="Delete Query"
                modalText="Are you sure you want to delete this query?"
            />
        </>
    );
}
