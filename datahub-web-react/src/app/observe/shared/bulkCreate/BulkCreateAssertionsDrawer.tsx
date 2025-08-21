import { Modal } from '@components';
import { message } from 'antd';
import React, { useState } from 'react';

import BulkCreateAssertionsProgress from '@app/observe/shared/bulkCreate/BulkCreateAssertionsProgress';
import { BulkCreateDatasetAssertionsSpec } from '@app/observe/shared/bulkCreate/constants';
import { BulkCreateAssertionsForm } from '@app/observe/shared/bulkCreate/form/BulkCreateAssertionsForm';
import { useBulkCreateDatasetAssertions } from '@app/observe/shared/bulkCreate/useBulkCreateDatasetAssertions';

type Props = {
    open: boolean;
    onClose: () => void;
};

export default function BulkCreateAssertionsDrawer({ open, onClose }: Props) {
    const [stage, setStage] = useState<'form' | 'submitted'>('form');

    // ----------- Actions ----------- //
    const { bulkCreateDatasetAssertions, progress } = useBulkCreateDatasetAssertions();

    const onSubmit = async (spec: BulkCreateDatasetAssertionsSpec) => {
        try {
            setStage('submitted');
            await bulkCreateDatasetAssertions(spec);
        } catch (error) {
            message.error(`Failed to create datasets with error: ${error}`, 5);
            setStage('form');
        }
    };

    // ----------- Render UI ----------- //
    return (
        <Modal
            width={1000}
            open={open}
            onCancel={onClose}
            title="Bulk Create Assertions"
            bodyStyle={{
                overflowY: 'auto',
                paddingTop: 0,
            }}
        >
            {stage === 'form' && <BulkCreateAssertionsForm onSubmit={onSubmit} />}
            {stage === 'submitted' && <BulkCreateAssertionsProgress progress={progress} onDone={onClose} />}
        </Modal>
    );
}
