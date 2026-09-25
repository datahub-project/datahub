import { DeleteOutlined, MoreOutlined } from '@ant-design/icons';
import { Dropdown, Menu, message } from 'antd';
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { ConfirmationModal } from '@app/sharedV2/modals/ConfirmationModal';

import { useDeleteQueryMutation } from '@graphql/query.generated';

const StyledMoreOutlined = styled(MoreOutlined)`
    font-size: 14px;
`;

type Props = {
    urn: string;
    onDeleted?: (urn: string) => void;
    index?: number;
};

export default function QueryCardDetailsMenu({ urn, onDeleted, index }: Props) {
    const { t } = useTranslation('entity.profile.queries');
    const { t: tc } = useTranslation('common.actions');
    const [deleteQueryMutation] = useDeleteQueryMutation();
    const [showConfirmationModal, setShowConfirmationModa] = useState(false);

    const deleteQuery = () => {
        deleteQueryMutation({ variables: { urn } })
            .then(({ errors }) => {
                if (!errors) {
                    message.success({
                        content: t('queryCard.deleteSuccess'),
                        duration: 3,
                    });
                    onDeleted?.(urn);
                }
            })
            .catch(() => {
                message.destroy();
                message.error({ content: t('queryCard.deleteError') });
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
                            <DeleteOutlined /> &nbsp; {tc('delete')}
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
                modalTitle={t('queryCard.deleteConfirmTitle')}
                modalText={t('queryCard.deleteConfirmBody')}
            />
        </>
    );
}
