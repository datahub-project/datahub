import { CheckCircleFilled, LoadingOutlined } from '@ant-design/icons';
import styled from 'styled-components';
import React, { useState } from 'react';
import { Button, Modal, notification } from 'antd';
import { useBatchVerifyFormMutation } from '../../../../../graphql/form.generated';
import { useEntityFormContext } from '../EntityFormContext';
import { pluralize } from '../../../../shared/textUtil';

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
    const [isWaitingToRefetch, setIsWaitingToRefetch] = useState(false);
    const [batchVerifyForm, { loading }] = useBatchVerifyFormMutation();
    const { formUrn, selectedEntities, setShouldRefetchSearchResults, setSelectedEntities } = useEntityFormContext();
    const shouldShowLoading = loading || isWaitingToRefetch;
    const numSelectedEntities = selectedEntities.length;

    function batchVerify() {
        batchVerifyForm({ variables: { input: { formUrn, assetUrns: selectedEntities.map((e) => e.urn) } } }).then(
            () => {
                setIsWaitingToRefetch(true);
                setTimeout(() => {
                    notification.success({
                        message: 'Success',
                        description: `You have successfully verified ${numSelectedEntities} ${
                            numSelectedEntities === 1 ? 'entity' : 'entities'
                        }`,
                        placement: 'bottomLeft',
                        duration: 3,
                        icon: <CheckCircleFilled style={{ color: '#078781' }} />,
                    });
                    setIsWaitingToRefetch(false);
                    setShouldRefetchSearchResults(true);
                    setSelectedEntities([]);
                    closeModal();
                }, 3000);
            },
        );
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
