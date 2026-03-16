import { Button, Descriptions, Input, InputNumber, Modal, Select, Space, Typography, message } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';

import { CompliancePropertyQualifiedName } from '@app/entity/shared/tabs/Dataset/Privacy/constant';
import {
    buildStructuredPropertyUrn,
    createRemoveMutation,
    createUpsertMutation,
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

type PendingRetentionValues = {
    column?: string;
    days?: string;
    jira?: string;
};

type EditRetentionProps = {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    structuredProps: any[];
    urn: string;
    onClose: () => void;
    refetchDataset: () => void;
    dateFieldNames: string[];
};

export function EditRetention({ structuredProps, urn, onClose, refetchDataset, dateFieldNames }: EditRetentionProps) {
    const [upsertProp, { loading: mutLoading }] = useUpsertStructuredPropertiesMutation();
    const [removeProp] = useRemoveStructuredPropertiesMutation();

    const currentColumn = String(
        getStructuredValue(structuredProps, CompliancePropertyQualifiedName.RetentionColumn) ?? '',
    );
    const currentDays = String(
        getStructuredValue(structuredProps, CompliancePropertyQualifiedName.RetentionDays) ?? '',
    );
    const currentJira = String(
        getStructuredValue(structuredProps, CompliancePropertyQualifiedName.RetentionJira) ?? '',
    );

    const [pending, setPending] = useState<PendingRetentionValues>({});
    const [confirmAction, setConfirmAction] = useState<'save' | 'discard' | null>(null);

    const columnPropUrn = buildStructuredPropertyUrn(CompliancePropertyQualifiedName.RetentionColumn);
    const daysPropUrn = buildStructuredPropertyUrn(CompliancePropertyQualifiedName.RetentionDays);
    const jiraPropUrn = buildStructuredPropertyUrn(CompliancePropertyQualifiedName.RetentionJira);

    const selectedColumn = pending.column !== undefined ? pending.column : currentColumn;
    const selectedDays = pending.days !== undefined ? pending.days : currentDays;
    const selectedJira = pending.jira !== undefined ? pending.jira : currentJira;

    const hasChanges =
        (pending.column !== undefined && pending.column !== currentColumn) ||
        (pending.days !== undefined && pending.days !== currentDays) ||
        (pending.jira !== undefined && pending.jira !== currentJira);

    const columnAllowedValues = dateFieldNames;

    const handleSaveAll = async () => {
        const upsertProperties: Array<{ urn: string; values: string[] }> = [];
        const removeUrns: string[] = [];

        if (pending.column !== undefined) {
            if (pending.column === '') {
                removeUrns.push(columnPropUrn);
            } else {
                upsertProperties.push({ urn: columnPropUrn, values: [pending.column] });
            }
        }

        if (pending.days !== undefined) {
            if (pending.days === '') {
                removeUrns.push(daysPropUrn);
            } else {
                upsertProperties.push({ urn: daysPropUrn, values: [pending.days] });
            }
        }

        if (pending.jira !== undefined) {
            if (pending.jira === '') {
                removeUrns.push(jiraPropUrn);
            } else {
                upsertProperties.push({ urn: jiraPropUrn, values: [pending.jira] });
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
            message.success('Retention updated successfully!');
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

    const renderFieldInput = (
        label: string,
        value: string,
        allowedValues: string[],
        onChange: (val: string) => void,
        isNumeric?: boolean,
    ) => {
        if (allowedValues.length > 0) {
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
        }

        if (isNumeric) {
            return (
                <InputNumber
                    value={value ? Number(value) : undefined}
                    onChange={(val) => onChange(val !== null && val !== undefined ? String(val) : '')}
                    style={{ width: '100%' }}
                    placeholder={`Enter ${label}`}
                    min={0}
                />
            );
        }

        return (
            <Input value={value} onChange={(e) => onChange(e.target.value)} placeholder={`Enter ${label}`} allowClear />
        );
    };

    const buildSummary = () => {
        const parts: string[] = [];
        if (pending.column !== undefined) parts.push(`Column: "${pending.column || '(cleared)'}"`);
        if (pending.days !== undefined) parts.push(`Days: "${pending.days || '(cleared)'}"`);
        if (pending.jira !== undefined) parts.push(`Jira: "${pending.jira || '(cleared)'}"`);
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
                <Descriptions.Item label="Retention Column">
                    {renderFieldInput('Retention Column', selectedColumn, columnAllowedValues, (val) =>
                        setPending((prev) => ({ ...prev, column: val })),
                    )}
                </Descriptions.Item>
                <Descriptions.Item label="Retention Days">
                    {renderFieldInput(
                        'Retention Days',
                        selectedDays,
                        [],
                        (val) => setPending((prev) => ({ ...prev, days: val })),
                        true,
                    )}
                </Descriptions.Item>
                <Descriptions.Item label="Retention Exception Jira">
                    {renderFieldInput('Retention Exception Jira', selectedJira, [], (val) =>
                        setPending((prev) => ({ ...prev, jira: val })),
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
                        You are about to update the following Retention fields: <strong>{buildSummary()}</strong>.
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
                        Retention fields will revert to their original values.
                        <br />
                        <br />
                        Are you sure?
                    </Text>
                )}
            </Modal>
        </Container>
    );
}

export default EditRetention;
