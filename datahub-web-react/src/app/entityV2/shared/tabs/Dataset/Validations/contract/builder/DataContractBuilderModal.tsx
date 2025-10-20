import { Modal, Typography, message } from 'antd';
import React, { useState } from 'react';

import { GenericEntityProperties } from '@app/entity/shared/types';
import { DataContractBuilder } from '@app/entityV2/shared/tabs/Dataset/Validations/contract/builder/DataContractBuilder';
import {
    DEFAULT_BUILDER_STATE,
    DataContractBuilderState,
} from '@app/entityV2/shared/tabs/Dataset/Validations/contract/builder/types';
import { buildProposeDataContractMutationVariables } from '@app/entityV2/shared/tabs/Dataset/Validations/contract/builder/utils';
import ClickOutside from '@app/shared/ClickOutside';
import analytics, { EntityActionType, EventType } from '@src/app/analytics';
import ProposalDescriptionModal from '@src/app/entityV2/shared/containers/profile/sidebar/ProposalDescriptionModal';
import { useAppConfig } from '@src/app/useAppConfig';
import { useProposeDataContractMutation } from '@src/graphql/contract.generated';

import { ActionRequestType, DataContract, DataContractProposalOperationType, EntityType } from '@types';

const modalStyle = {};
const modalBodyStyle = {
    paddingRight: 0,
    paddingLeft: 0,
    paddingBottom: 0,
    paddingTop: 0,
    height: '70vh',
    'overflow-x': 'auto',
};

type Props = {
    entityUrn: string;
    initialState?: DataContractBuilderState;
    onSubmit?: (contract: DataContract) => void;
    onPropose?: () => void;
    onCancel?: () => void;
    entityType?: EntityType;
    entityData?: GenericEntityProperties | null;
};

/**
 * This component is a modal used for constructing new Data Contracts
 */
export const DataContractBuilderModal = ({
    entityUrn,
    initialState,
    onSubmit,
    onPropose,
    onCancel,
    entityType,
    entityData,
}: Props) => {
    const isEditing = initialState !== undefined;
    const titleText = isEditing ? 'Edit Data Contract' : 'New Data Contract';

    const [builderState, setBuilderState] = useState(initialState || DEFAULT_BUILDER_STATE);

    const [proposeDataContractMutation] = useProposeDataContractMutation();

    const [showProposeModal, setShowProposeModal] = useState(false);
    const { config } = useAppConfig();
    const { showTaskCenterRedesign } = config.featureFlags;

    const modalClosePopup = () => {
        Modal.confirm({
            title: 'Exit Editor',
            content: `Are you sure you want to exit the editor? All changes will be lost`,
            onOk() {
                onCancel?.();
            },
            onCancel() {},
            okText: 'Yes',
            maskClosable: true,
            closable: true,
        });
    };

    /**
     * Proposes the upsert to the Data Contract for an entity
     */
    const proposeUpsertDataContract = (description?: string) => {
        return proposeDataContractMutation({
            variables: buildProposeDataContractMutationVariables(
                DataContractProposalOperationType.Overwrite,
                entityUrn,
                builderState,
                description,
            ),
        })
            .then(({ errors }) => {
                if (!errors) {
                    analytics.event({
                        type: EventType.EntityActionEvent,
                        actionType: EntityActionType.ProposalCreated,
                        actionQualifier: ActionRequestType.DataContract,
                        entityType,
                        entityUrn,
                    });
                    message.success({
                        content: `Proposed Data Contract!`,
                        duration: 3,
                    });
                    onPropose?.();
                    setShowProposeModal(false);
                }
            })
            .catch(() => {
                message.destroy();
                message.error({ content: 'Failed to propose Data Contract! An unexpected error occurred' });
            });
    };

    const handlePropose = () => {
        if (showTaskCenterRedesign) {
            setShowProposeModal(true);
        } else {
            proposeUpsertDataContract();
        }
    };

    return (
        <ClickOutside onClickOutside={modalClosePopup} wrapperClassName="data-contract-builder-modal">
            {!showProposeModal && (
                <Modal
                    wrapClassName="data-contract-builder-modal"
                    width={800}
                    footer={null}
                    title={<Typography.Text>{titleText}</Typography.Text>}
                    style={modalStyle}
                    bodyStyle={modalBodyStyle}
                    visible
                    onCancel={onCancel}
                >
                    <DataContractBuilder
                        entityUrn={entityUrn}
                        initialState={initialState}
                        onSubmit={onSubmit}
                        onCancel={onCancel}
                        builderState={builderState}
                        setBuilderState={setBuilderState}
                        handlePropose={handlePropose}
                        entityData={entityData}
                    />
                </Modal>
            )}
            {showProposeModal && (
                <ProposalDescriptionModal
                    onPropose={proposeUpsertDataContract}
                    onCancel={() => setShowProposeModal(false)}
                />
            )}
        </ClickOutside>
    );
};
