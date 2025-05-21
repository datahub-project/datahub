import { Button, Modal } from 'antd';
import React from 'react';

import { SchemaSummary } from '@app/entityV2/shared/tabs/Dataset/Validations/SchemaSummary';

import { SchemaMetadata } from '@types';

const modalStyle = {
    top: 40,
};

const modalBodyStyle = {
    padding: 0,
};

type Props = {
    schema: SchemaMetadata;
    onClose: () => void;
};

export const SchemaSummaryModal = ({ schema, onClose }: Props) => {
    return (
        <Modal
            width={800}
            style={modalStyle}
            bodyStyle={modalBodyStyle}
            title="View Schema Assertion"
            visible
            onCancel={onClose}
            footer={<Button onClick={onClose}>Close</Button>}
        >
            <SchemaSummary schema={schema} />
        </Modal>
    );
};
