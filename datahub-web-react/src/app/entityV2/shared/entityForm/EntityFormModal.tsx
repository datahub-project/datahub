/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { CloseOutlined } from '@ant-design/icons';
import { Modal } from 'antd';
import React from 'react';
import styled from 'styled-components';

import EntityForm from '@app/entityV2/shared/entityForm/EntityForm';
import EntityFormContextProvider from '@src/app/entity/shared/entityForm/EntityFormContextProvider';
import FormPageHeader from '@src/app/entity/shared/entityForm/FormHeader/FormPageHeader';

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
