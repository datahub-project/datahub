/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Modal } from 'antd';
import React from 'react';

import FormSelector from '@app/entity/shared/entityForm/FormSelectionModal/FormSelector';

interface Props {
    isFormSelectionModalVisible: boolean;
    hideFormSelectionModal: () => void;
    selectFormUrn: (urn: string) => void;
}

export default function FormSelectionModal({
    isFormSelectionModalVisible,
    hideFormSelectionModal,
    selectFormUrn,
}: Props) {
    return (
        <Modal open={isFormSelectionModalVisible} onCancel={hideFormSelectionModal} footer={null}>
            <FormSelector selectFormUrn={selectFormUrn} />
        </Modal>
    );
}
