import { Button, Descriptions, Modal, Select, Space, Typography, message } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';

import { CompliancePropertyQualifiedName } from '@app/entity/shared/tabs/Dataset/Privacy/constant';
import {
    buildStructuredPropertyUrn,
    createRemoveMutation,
    createUpsertMutation,
    getStructuredPropAllowedValues,
    getStructuredValue,
} from '@app/entity/shared/tabs/Dataset/Privacy/utils';

import {
    useRemoveStructuredPropertiesMutation,
    useUpsertStructuredPropertiesMutation,
} from '@graphql/structuredProperties.generated';

const { Text } = Typography;
const { Option } = Select;

const Container = styled.div`
    padding: 24px;
`;

const ActionSpace = styled(Space)`
    margin-top: 24px;
    width: 100%;
    justify-content: flex-end;
`;

type PendingScrubbingValues = {
    operation?: string;
    state?: string;
    status?: string;
};

type EditScrubbingProps = {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    structuredProps: any[];
    urn: string;
    onClose: () => void;
    refetchDataset: () => void;
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    scrubbingOpPropData: any;
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    scrubbingStatePropData: any;
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    scrubbingStatusPropData: any;
};

export function EditScrubbing({
    structuredProps,
    urn,
    onClose,
    refetchDataset,
    scrubbingOpPropData,
    scrubbingStatePropData,
    scrubbingStatusPropData,
}: EditScrubbingProps) {
    const [upsertProp, { loading: mutLoading }] = useUpsertStructuredPropertiesMutation();
    const [removeProp] = useRemoveStructuredPropertiesMutation();

    const currentOp = String(
        getStructuredValue(structuredProps, CompliancePropertyQualifiedName.ScrubbingOp) ?? '',
    );
    const currentState = String(
        getStructuredValue(structuredProps, CompliancePropertyQualifiedName.ScrubbingState) ?? '',
    );
    const currentStatus = String(
        getStructuredValue(structuredProps, CompliancePropertyQualifiedName.ScrubbingStatus) ?? '',
    );

    const [pending, setPending] = useState<PendingScrubbingValues>({});
    const [confirmAction, setConfirmAction] = useState<'save' | 'discard' | null>(null);

    const opPropUrn = buildStructuredPropertyUrn(CompliancePropertyQualifiedName.ScrubbingOp);
    const statePropUrn = buildStructuredPropertyUrn(CompliancePropertyQualifiedName.ScrubbingState);
    const statusPropUrn = buildStructuredPropertyUrn(CompliancePropertyQualifiedName.ScrubbingStatus);

    const selectedOp = pending.operation !== undefined ? pending.operation : currentOp;
    const selectedState = pending.state !== undefined ? pending.state : currentState;
    const selectedStatus = pending.status !== undefined ? pending.status : currentStatus;

    const hasChanges =
        (pending.operation !== undefined && pending.operation !== currentOp) ||
        (pending.state !== undefined && pending.state !== currentState) ||
        (pending.status !== undefined && pending.status !== currentStatus);

    const opAllowedValues = getStructuredPropAllowedValues(scrubbingOpPropData);
    const stateAllowedValues = getStructuredPropAllowedValues(scrubbingStatePropData);
    const statusAllowedValues = getStructuredPropAllowedValues(scrubbingStatusPropData);

    const handleSaveAll = async () => {
        const upsertProperties: Array<{ urn: string; values: string[] }> = [];
        const removeUrns: string[] = [];

        if (pending.operation !== undefined) {
            if (pending.operation === '') {
                removeUrns.push(opPropUrn);
            } else {
                upsertProperties.push({ urn: opPropUrn, values: [pending.operation] });
            }
        }

        if (pending.state !== undefined) {
            if (pending.state === '') {
                removeUrns.push(statePropUrn);
            } else {
                upsertProperties.push({ urn: statePropUrn, values: [pending.state] });
            }
        }

        if (pending.status !== undefined) {
            if (pending.status === '') {
                removeUrns.push(statusPropUrn);
            } else {
                upsertProperties.push({ urn: statusPropUrn, values: [pending.status] });
            }
        }

        const mutations: Promise<unknown>[] = [];

        if (upsertProperties.length > 0) {
            mutations.push(createUpsertMutation(urn, upsertProperties, upsertProp));
        }

        removeUrns.forEach((propUrn) => {
            mutations.push(createRemoveMutation(urn, propUrn, removeProp));
        });

        try {
            await Promise.all(mutations);
            message.success('Scrubbing updated successfully!');
            setPending({});
            refetchDataset();
            onClose();
        } catch (err: unknown) {
            const errorMessage = err instanceof Error ? err.message : 'Unknown error';
            message.error(`Save failed: ${errorMessage}`);
        }
    };

    const handleDiscardAll = () => {
        setPending({});
        refetchDataset();
        onClose();
        message.info('Changes discarded');
    };

    const handleConfirmAction = () => {
        if (confirmAction === 'save') {
            handleSaveAll();
        } else {
            handleDiscardAll();
        }
        setConfirmAction(null);
    };

    const renderSelectField = (label: string, value: string, allowedValues: string[], onChange: (val: string) => void) => {
        return (
            <Select
                value={value || undefined}
                onChange={onChange}
                style={{ width: '100%' }}
                placeholder={`Select ${label}`}
                allowClear
                onClear={() => onChange('')}
                showSearch
            >
                {allowedValues.map((opt) => (
                    <Option key={opt} value={opt}>
                        {opt}
                    </Option>
                ))}
            </Select>
        );
    };

    const buildSummary = () => {
        const parts: string[] = [];
        if (pending.operation !== undefined) parts.push(`Operation: "${pending.operation || '(cleared)'}"`);
        if (pending.state !== undefined) parts.push(`State: "${pending.state || '(cleared)'}"`);
        if (pending.status !== undefined) parts.push(`Status: "${pending.status || '(cleared)'}"`);
        return parts.join(', ');
    };

    return (
        <Container>
            {!hasChanges && (
                <Button type="primary" onClick={onClose}>
                    Back
                </Button>
            )}

            <Descriptions bordered column={1}>
                <Descriptions.Item label="Scrubbing Operation">
                    {renderSelectField('Scrubbing Operation', selectedOp, opAllowedValues, (val) =>
                        setPending((prev) => ({ ...prev, operation: val })),
                    )}
                </Descriptions.Item>
                <Descriptions.Item label="Scrubbing State">
                    {renderSelectField('Scrubbing State', selectedState, stateAllowedValues, (val) =>
                        setPending((prev) => ({ ...prev, state: val })),
                    )}
                </Descriptions.Item>
                <Descriptions.Item label="Scrubbing Status">
                    {renderSelectField('Scrubbing Status', selectedStatus, statusAllowedValues, (val) =>
                        setPending((prev) => ({ ...prev, status: val })),
                    )}
                </Descriptions.Item>
            </Descriptions>

            {hasChanges && (
                <ActionSpace>
                    <Button type="primary" onClick={() => setConfirmAction('save')} loading={mutLoading}>
                        Save All Changes
                    </Button>
                    <Button danger onClick={() => setConfirmAction('discard')} disabled={mutLoading}>
                        Discard All
                    </Button>
                </ActionSpace>
            )}

            <Modal
                title={confirmAction === 'save' ? 'Confirm Save All Changes' : 'Confirm Discard All Changes'}
                open={!!confirmAction}
                onOk={handleConfirmAction}
                onCancel={() => setConfirmAction(null)}
                okText={confirmAction === 'save' ? 'Yes, Save All' : 'Yes, Discard All'}
                cancelText="No, Cancel"
                okButtonProps={{
                    danger: confirmAction === 'discard',
                    loading: mutLoading && confirmAction === 'save',
                }}
                width={500}
            >
                {confirmAction === 'save' ? (
                    <Text>
                        You are about to update the following Scrubbing fields: <strong>{buildSummary()}</strong>.
                        <br />
                        <br />
                        This action cannot be undone after saving.
                        <br />
                        <br />
                        Are you sure?
                    </Text>
                ) : (
                    <Text>
                        You are about to discard all unsaved changes.
                        <br />
                        <br />
                        Scrubbing fields will revert to their original values.
                        <br />
                        <br />
                        Are you sure?
                    </Text>
                )}
            </Modal>
        </Container>
    );
}

export default EditScrubbing;