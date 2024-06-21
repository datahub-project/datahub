import React, { useState } from 'react';

import { CheckCircleFilled, LoadingOutlined } from '@ant-design/icons';
import styled from 'styled-components';
import { Button, Modal, notification } from 'antd';

import { useBatchVerifyFormMutation } from '../../../../../graphql/form.generated';
import { useEntityFormContext } from '../EntityFormContext';

import { pluralize } from '../../../../shared/textUtil';
import analytics, { DocRequestView, EventType } from '../../../../analytics';

const ModalContent = styled.div`
    font-size: 14px;
`;

const Title = styled.div`
    font-weight: 600;
    margin-bottom: 8px;
`;

const ButtonsWrapper = styled.div`
    margin-top: 12px;
    display: flex;
    justify-content: flex-end;
`;

const CancelButton = styled(Button)`
    margin-right: 8px;
`;

const StyledLoading = styled(LoadingOutlined)`
    margin-right: 8px;
`;

interface Props {
    isVerifyModalVisible: boolean;
    closeModal: () => void;
}

export default function BulkVerifyModal({ isVerifyModalVisible, closeModal }: Props) {
    const {
        submission: { handleBulkVerifySubmission },
        form: { formUrn },
        entity: { selectedEntities, setSelectedEntities },
    } = useEntityFormContext();

    const [isSubmitting, setIsSubmitting] = useState(false);
    const [batchVerifyForm, { loading }] = useBatchVerifyFormMutation();

    const shouldShowLoading = loading || isSubmitting;
    const numSelectedEntities = selectedEntities.length;

    function batchVerify() {
        setIsSubmitting(true);
        const selectedEntityUrns = selectedEntities.map((e) => e.urn);
        batchVerifyForm({ variables: { input: { formUrn, assetUrns: selectedEntityUrns } } }).then(() => {
            analytics.event({
                type: EventType.CompleteVerification,
                source: DocRequestView.BulkVerify,
                numAssets: 1,
            });
            handleBulkVerifySubmission(selectedEntityUrns);
            setIsSubmitting(false);
            notification.success({
                message: 'Success',
                description: `You have successfully verified ${numSelectedEntities} ${
                    numSelectedEntities === 1 ? 'entity' : 'entities'
                }`,
                placement: 'bottomLeft',
                duration: 3,
                icon: <CheckCircleFilled style={{ color: '#078781' }} />,
            });
            setSelectedEntities([]);
            closeModal();
        });
    }

    return (
        <Modal open={isVerifyModalVisible} title={null} footer={null} onCancel={closeModal}>
            <ModalContent>
                <Title>
                    Verify {numSelectedEntities} {pluralize(numSelectedEntities, 'Asset')}
                </Title>
                <div>
                    This action will mark the documentation for {} as verified for {numSelectedEntities}{' '}
                    {pluralize(numSelectedEntities, 'asset')}. Would you like to proceed?
                </div>
                <ButtonsWrapper>
                    <CancelButton onClick={closeModal} disabled={shouldShowLoading}>
                        Cancel
                    </CancelButton>
                    <Button type="primary" onClick={batchVerify} disabled={shouldShowLoading}>
                        {!shouldShowLoading && (
                            <>
                                Verify {numSelectedEntities} {pluralize(numSelectedEntities, 'Asset')}
                            </>
                        )}
                        {shouldShowLoading && (
                            <>
                                <StyledLoading /> Applying Verification
                            </>
                        )}
                    </Button>
                </ButtonsWrapper>
            </ModalContent>
        </Modal>
    );
}
