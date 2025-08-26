import { CloseOutlined } from '@ant-design/icons';
import { Modal } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { useUserContext } from '@app/context/useUserContext';
import EntityFormContextProvider from '@app/entity/shared/entityForm/EntityFormContextProvider';
import FormPageHeader from '@app/entity/shared/entityForm/FormHeader/FormPageHeader';
import EntityForm from '@app/entityV2/shared/entityForm/EntityForm';
import { colors } from '@src/alchemy-components';

const StyledModal = styled(Modal)`
    &&& .ant-modal-content {
        display: flex;
        flex-direction: column;
        height: calc(100vh);
    }

    .ant-modal-header {
        padding: 0;
    }

    .ant-modal-body {
        flex: 1;
        max-height: 100%;
        overflow: hidden;
        padding: 0;
        display: flex;
        flex-direction: column;
    }
`;

const StyledClose = styled(CloseOutlined)`
    && {
        color: ${colors.gray[600]};
        font-size: 24px;
        margin: 18px 12px 0 0;
    }
`;

const EntityFormWrapper = styled.div`
    flex: 1;
    overflow: auto;
    display: flex;
`;

interface Props {
    selectedFormUrn: string | null;
    isFormVisible: boolean;
    hideFormModal: () => void;
}

export default function EntityFormModal({ selectedFormUrn, isFormVisible, hideFormModal }: Props) {
    const { refetchUnfinishedTaskCount } = useUserContext();

    // Refetch unfinished tasks when closing modal
    const handleClose = () => {
        refetchUnfinishedTaskCount();
        hideFormModal();
    };

    return (
        <StyledModal
            open={isFormVisible}
            onCancel={handleClose}
            footer={null}
            title={null}
            closeIcon={<StyledClose />}
            style={{ top: 0, height: '100vh', minWidth: '100vw' }}
            destroyOnClose
        >
            <EntityFormContextProvider formUrn={selectedFormUrn || ''}>
                <FormPageHeader />
                <EntityFormWrapper>
                    <EntityForm formUrn={selectedFormUrn || ''} closeModal={handleClose} />
                </EntityFormWrapper>
            </EntityFormContextProvider>
        </StyledModal>
    );
}
