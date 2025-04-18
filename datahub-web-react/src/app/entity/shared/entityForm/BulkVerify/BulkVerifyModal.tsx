import { CheckCircleFilled, LoadingOutlined } from '@ant-design/icons';
import { Button, Modal, notification } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';

import analytics, { DocRequestView, EventType } from '@app/analytics';
import { useEntityFormContext } from '@app/entity/shared/entityForm/EntityFormContext';
import { BULK_VERIFY_ID } from '@app/entity/shared/entityForm/useEntityFormTasks';
import { pluralize } from '@app/shared/textUtil';
import { EntityType } from '@src/types.generated';

import { useAsyncBatchVerifyFormMutation, useBatchVerifyFormMutation } from '@graphql/form.generated';

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
        submission: { handleBulkVerifySubmission, handleAsyncBatchSubmit },
        form: { formUrn },
        filter: { formFilter, orFilters },
        entity: { selectedEntities, setSelectedEntities, areAllEntitiesSelected, setAreAllEntitiesSelected },
        search: { results },
    } = useEntityFormContext();

    const [isSubmitting, setIsSubmitting] = useState(false);
    const [batchVerifyForm, { loading }] = useBatchVerifyFormMutation();
    const [asyncBatchVerifyForm, { loading: asyncVerifyLoading }] = useAsyncBatchVerifyFormMutation();

    const shouldShowLoading = loading || isSubmitting || asyncVerifyLoading;
    const numSelectedEntities = selectedEntities.length;
    const totalCount = results.searchAcrossEntities?.total || 0;

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

    function asyncBatchVerify() {
        setIsSubmitting(true);
        const types = results.searchAcrossEntities?.facets
            ?.find((f) => f.field === '_entityType')
            ?.aggregations.filter((agg) => !!agg.count)
            .map((agg) => agg.value) as EntityType[];
        asyncBatchVerifyForm({
            variables: {
                input: {
                    types: types || [],
                    formFilter,
                    orFilters,
                    formUrn,
                    taskInput: { taskName: 'Verify form' },
                },
            },
        }).then((result) => {
            analytics.event({
                type: EventType.CompleteVerification,
                source: DocRequestView.BulkVerify,
                numAssets: 1,
            });
            setIsSubmitting(false);
            notification.success({
                message: 'Success',
                description: `Started task to verify form for ${totalCount} ${pluralize(
                    selectedEntities.length,
                    'asset',
                )}.`,
                placement: 'bottomLeft',
                duration: 3,
                icon: <CheckCircleFilled style={{ color: '#078781' }} />,
            });
            setSelectedEntities([]);
            setAreAllEntitiesSelected(false);
            closeModal();
            if (result.data?.asyncBatchVerifyForm.taskUrn) {
                handleAsyncBatchSubmit(result.data.asyncBatchVerifyForm.taskUrn, BULK_VERIFY_ID);
            }
        });
    }

    function handleSubmit() {
        if (areAllEntitiesSelected) {
            asyncBatchVerify();
        } else {
            batchVerify();
        }
    }

    const numEntitiesVerified = areAllEntitiesSelected ? totalCount : numSelectedEntities;

    return (
        <Modal open={isVerifyModalVisible} title={null} footer={null} onCancel={closeModal}>
            <ModalContent>
                <Title>
                    Verify {numEntitiesVerified} {pluralize(numEntitiesVerified, 'Asset')}
                </Title>
                <div>
                    This action will mark the documentation for {} as verified for {numEntitiesVerified}{' '}
                    {pluralize(numEntitiesVerified, 'asset')}. Would you like to proceed?
                </div>
                <ButtonsWrapper>
                    <CancelButton onClick={closeModal} disabled={shouldShowLoading}>
                        Cancel
                    </CancelButton>
                    <Button type="primary" onClick={handleSubmit} disabled={shouldShowLoading}>
                        {!shouldShowLoading && (
                            <>
                                Verify {numEntitiesVerified} {pluralize(numEntitiesVerified, 'Asset')}
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
