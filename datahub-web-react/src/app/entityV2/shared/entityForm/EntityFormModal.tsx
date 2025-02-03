import { CloseOutlined } from '@ant-design/icons';
import React from 'react';
import EntityFormContextProvider from '@src/app/entity/shared/entityForm/EntityFormContextProvider';
import FormPageHeader from '@src/app/entity/shared/entityForm/FormHeader/FormPageHeader';
import { Modal } from 'antd';
import styled from 'styled-components';
import EntityForm from './EntityForm';

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
    }
`;

const StyledClose = styled(CloseOutlined)`
    && {
        color: white;
        font-size: 24px;
        margin: 18px 12px 0 0;
    }
`;

interface Props {
    selectedFormUrn: string | null;
    isFormVisible: boolean;
    hideFormModal: () => void;
}

export default function EntityFormModal({ selectedFormUrn, isFormVisible, hideFormModal }: Props) {
    return (
        <EntityFormContextProvider formUrn={selectedFormUrn || ''}>
            <StyledModal
                open={isFormVisible}
                onCancel={hideFormModal}
                footer={null}
                title={<FormPageHeader />}
                closeIcon={<StyledClose />}
                style={{ top: 0, height: '100vh', minWidth: '100vw' }}
                destroyOnClose
            >
                <EntityForm formUrn={selectedFormUrn || ''} />
            </StyledModal>
        </EntityFormContextProvider>
    );
}
