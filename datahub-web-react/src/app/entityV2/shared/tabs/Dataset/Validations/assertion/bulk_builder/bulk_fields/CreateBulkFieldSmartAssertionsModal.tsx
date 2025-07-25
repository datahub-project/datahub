import { Modal, Text, colors } from '@components';
import { Sparkle } from 'phosphor-react';
import React, { useState } from 'react';
import styled from 'styled-components';

import { EligibleFieldColumn } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/steps/field/utils';
import { CreateBulkFieldSmartAssertionsForm } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/bulk_builder/bulk_fields/CreateBulkFieldSmartAssertionsForm';
import { CreateBulkFieldSmartAssertionsProgress } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/bulk_builder/bulk_fields/CreateBulkFieldSmartAssertionsProgress';
import {
    BulkFieldAssertionSpec,
    useBulkCreateFieldAssertions,
} from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/bulk_builder/bulk_fields/useBulkCreateFieldAssertions';

const TitleWrapper = styled.div({
    display: 'flex',
    alignItems: 'center',
    gap: 8,
});

type Props = {
    entityUrn: string;
    open: boolean;
    onClose: () => void;
    onSuccess: () => void;
    columns: EligibleFieldColumn[];
};

export const CreateBulkFieldSmartAssertionsModal = ({ entityUrn, open, onClose, onSuccess, columns }: Props) => {
    const { progressReport, upsertBulkFieldAssertions } = useBulkCreateFieldAssertions();

    const [stage, setStage] = useState<'configuration' | 'submitted'>('configuration');

    const onSubmit = (assertionSpec: BulkFieldAssertionSpec) => {
        setStage('submitted');
        upsertBulkFieldAssertions(assertionSpec);
    };

    return (
        <Modal
            open={open}
            onCancel={onClose}
            bodyStyle={{
                maxHeight: '90vh',
                overflowY: 'auto',
            }}
            title={
                <TitleWrapper>
                    <Sparkle size={16} color={colors.blue[500]} />
                    <Text>Bulk-Create Column Metric Smart Assertions</Text>
                </TitleWrapper>
            }
            width={800}
        >
            {stage === 'configuration' && (
                <CreateBulkFieldSmartAssertionsForm entityUrn={entityUrn} columns={columns} onSubmit={onSubmit} />
            )}
            {stage === 'submitted' && (
                <CreateBulkFieldSmartAssertionsProgress progress={progressReport} onDone={onSuccess} />
            )}
        </Modal>
    );
};
