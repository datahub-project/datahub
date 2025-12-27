import { Button, Form, Modal, message } from 'antd';
import React, { useState } from 'react';

import { handleBatchError } from '@app/entity/shared/utils';
import {
    useAddEntityToOrganizationsMutation,
    useRemoveEntityFromOrganizationsMutation,
} from '@graphql/mutations.generated';
import { OrganizationPicker } from '@app/organization/OrganizationPicker';
import { ModalButtonContainer } from '@app/shared/button/styledComponents';
import { getModalDomContainer } from '@app/utils/focus';

export enum OperationType {
    ADD,
    REMOVE,
}

interface Props {
    open: boolean;
    onCloseModal: () => void;
    urns: string[];
    operationType?: OperationType;
    onOkOverride?: (result: string[]) => void;
}

export const BulkAddOrganizationModal = ({
    open,
    onCloseModal,
    urns,
    operationType = OperationType.ADD,
    onOkOverride,
}: Props) => {
    const [selectedOrgUrns, setSelectedOrgUrns] = useState<string[]>([]);
    const [addEntityToOrganizationsMutation] = useAddEntityToOrganizationsMutation();
    const [removeEntityFromOrganizationsMutation] = useRemoveEntityFromOrganizationsMutation();
    const [loading, setLoading] = useState(false);

    const handleOk = async () => {
        if (onOkOverride) {
            onOkOverride(selectedOrgUrns);
            return;
        }

        setLoading(true);
        try {
            // Iterate over each entity and apply the organization change
            // This is a temporary solution until we have a batch mutation
            const promises = urns.map((entityUrn) => {
                if (operationType === OperationType.ADD) {
                    return addEntityToOrganizationsMutation({
                        variables: {
                            entityUrn,
                            organizationUrns: selectedOrgUrns,
                        },
                    });
                }
                return removeEntityFromOrganizationsMutation({
                    variables: {
                        entityUrn,
                        organizationUrns: selectedOrgUrns,
                    },
                });
            });

            await Promise.all(promises);

            message.success({
                content: `Successfully ${operationType === OperationType.ADD ? 'added' : 'removed'} organizations!`,
                duration: 2,
            });
            onCloseModal();
            setSelectedOrgUrns([]);
        } catch (e: unknown) {
            message.destroy();
            message.error(
                handleBatchError(urns, e, {
                    content: `Failed to ${operationType === OperationType.ADD ? 'add' : 'remove'} organizations`,
                    duration: 3,
                }),
            );
        } finally {
            setLoading(false);
        }
    };

    const handleCancel = () => {
        setSelectedOrgUrns([]);
        onCloseModal();
    };

    return (
        <Modal
            title={`${operationType === OperationType.ADD ? 'Add' : 'Remove'} Organizations`}
            open={open}
            onCancel={handleCancel}
            footer={
                <ModalButtonContainer>
                    <Button type="text" onClick={handleCancel}>
                        Cancel
                    </Button>
                    <Button
                        type="primary"
                        onClick={handleOk}
                        disabled={selectedOrgUrns.length === 0 || loading}
                        loading={loading}
                    >
                        {operationType === OperationType.ADD ? 'Add' : 'Remove'}
                    </Button>
                </ModalButtonContainer>
            }
            getContainer={getModalDomContainer}
        >
            <Form component={false}>
                <Form.Item label="Organizations">
                    <OrganizationPicker
                        selectedUrns={selectedOrgUrns}
                        onChange={setSelectedOrgUrns}
                        placeholder="Search for organizations..."
                    />
                </Form.Item>
            </Form>
        </Modal>
    );
};
