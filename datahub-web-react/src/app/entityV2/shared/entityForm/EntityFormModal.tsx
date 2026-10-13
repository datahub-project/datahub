import { X } from '@phosphor-icons/react/dist/csr/X';
import { Modal } from 'antd';
import React from 'react';
import styled from 'styled-components';

import EntityForm from '@app/entityV2/shared/entityForm/EntityForm';
// eslint-disable-next-line no-restricted-imports -- no semantic token maps to gray[600] for this use case (see StyledClose below); direct color import is intentional here.
import { colors } from '@src/alchemy-components';
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

// eslint-disable-next-line rulesdir/no-hardcoded-colors -- no inverse-surface token in design
// system (see FormPageHeader's Header); this modal's header is plain white, not a colored fill,
// so textOnFillDefault (near white) rendered invisibly here. Matches FormPageHeader's own title color.
const StyledClose = styled(X)`
    && {
        color: ${colors.gray[600]};
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
