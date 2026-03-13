import React, { useMemo, useState } from 'react';
import { Button, Descriptions, message, Modal, Select, Space, Table, Tag, Typography } from 'antd';
import type { ColumnsType } from 'antd/es/table';
import styled from 'styled-components';

import {
    useRemoveStructuredPropertiesMutation,
    useUpsertStructuredPropertiesMutation,
} from '@graphql/structuredProperties.generated';

import { CompliancePropertyQualifiedName } from '@app/entity/shared/tabs/Dataset/Privacy/constant';
import {
    buildSchemaFieldUrn,
    buildStructuredPropertyUrn,
    createRemoveMutation,
    createUpsertMutation,
    getStructuredPropAllowedValues,
    getStructuredValue,
} from '@app/entity/shared/tabs/Dataset/Privacy/utils';

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

// eslint-disable-next-line @typescript-eslint/no-explicit-any
type SchemaField = any;

type EditAnnotationProps = {
    fields: SchemaField[];
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    structuredProps: any[];
    urn: string;
    refetch: () => void;
    onClose: () => void;
    refetchDataset: () => void;
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    getMeData: any;
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    piiAnnotationPropData: any;
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    hasPersonalDataPropData: any;
};

export function EditAnnotation({
    fields,
    structuredProps,
    urn,
    refetch,
    onClose,
    refetchDataset,
    getMeData,
    piiAnnotationPropData,
    hasPersonalDataPropData,
}: EditAnnotationProps) {
    const [upsertProp, { loading: mutLoading }] = useUpsertStructuredPropertiesMutation();
    const [removeProp] = useRemoveStructuredPropertiesMutation();

    const [pendingChanges, setPendingChanges] = useState<Record<string, string[]>>({});
    const [pendingHasPersonalDataChange, setPendingHasPersonalDataChange] = useState<string | undefined>();
    const [confirmAction, setConfirmAction] = useState<'save' | 'discard' | null>(null);

    const targetPropUrn = buildStructuredPropertyUrn(CompliancePropertyQualifiedName.PiiAnnotation);

    const annotationsMap = useMemo(() => {
        const map = new Map<string, string[]>();
        fields.forEach((field) => {
            field.schemaFieldEntity?.structuredProperties?.properties.forEach((prop) => {
                if (prop.structuredProperty?.urn === targetPropUrn && prop.structuredProperty?.exists && prop.values?.length) {
                    const vals = prop.values.map((v) => v.stringValue || v.value || '').filter(Boolean);
                    map.set(field.fieldPath, vals);
                }
            });
        });
        return map;
    }, [fields, targetPropUrn]);

    const hasPersonalData = getStructuredValue(structuredProps, CompliancePropertyQualifiedName.HasPersonalData);

    const hasPersonalDataChanged = () => {
        return pendingHasPersonalDataChange !== undefined && pendingHasPersonalDataChange !== hasPersonalData;
    };

    const hasChanges = Object.keys(pendingChanges).length > 0 || hasPersonalDataChanged();

    const handleChange = (fieldPath: string, values: string[]) => {
        setPendingChanges((prev) => ({
            ...prev,
            [fieldPath]: values,
        }));
    };

    const handleHasPersonalDataChange = (value: string | undefined) => {
        setPendingHasPersonalDataChange(value);
    };

    const handleSaveAll = async () => {
        const mutations: Promise<unknown>[] = [];

        if (hasPersonalDataChanged() && pendingHasPersonalDataChange !== 'Yes') {
            Array.from(annotationsMap.keys()).forEach((fieldPath) => {
                mutations.push(createRemoveMutation(buildSchemaFieldUrn(urn, fieldPath), targetPropUrn, removeProp));
            });
        } else {
            Object.entries(pendingChanges).forEach(([fieldPath, values]) => {
                const schemaFieldUrn = buildSchemaFieldUrn(urn, fieldPath);

                if (values.length === 0) {
                    mutations.push(createRemoveMutation(schemaFieldUrn, targetPropUrn, removeProp));
                } else {
                    mutations.push(createUpsertMutation(schemaFieldUrn, [{ urn: targetPropUrn, values }], upsertProp));
                }
            });
        }

        if (hasPersonalDataChanged() || mutations.length > 0) {
            const datasetProperties: Array<{ urn: string; values: string[] }> = [
                {
                    urn: buildStructuredPropertyUrn(CompliancePropertyQualifiedName.HasPersonalDataUpdatedAt),
                    values: [new Date().toISOString().split('T')[0]],
                },
                {
                    urn: buildStructuredPropertyUrn(CompliancePropertyQualifiedName.HasPersonalDataUpdatedBy),
                    values: [getMeData?.me?.corpUser?.username ?? ''],
                },
            ];
            if (hasPersonalDataChanged()) {
                datasetProperties.unshift({
                    urn: buildStructuredPropertyUrn(CompliancePropertyQualifiedName.HasPersonalData),
                    values: [pendingHasPersonalDataChange!],
                });
            }
            mutations.push(createUpsertMutation(urn, datasetProperties, upsertProp));
        }

        try {
            await Promise.all(mutations);
            message.success('You have annotated a dataset, good job!');
            setPendingChanges({});
            setPendingHasPersonalDataChange(undefined);
            refetchDataset();
            refetch();
            onClose();
        } catch (err: any) {
            message.error(`Save failed: ${err.message}`);
        }
    };

    const handleDiscardAll = () => {
        setPendingChanges({});
        setPendingHasPersonalDataChange(undefined);
        refetchDataset();
        refetch();
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

    const isFieldAnnotationsVisible =
        pendingHasPersonalDataChange !== undefined
            ? pendingHasPersonalDataChange === 'Yes'
            : hasPersonalData === 'Yes';

    const columns: ColumnsType<any> = [
        {
            title: 'Field Name',
            dataIndex: 'fieldPath',
            key: 'fieldPath',
            width: 300,
            render: (text: string) => <strong>{text}</strong>,
        },
        {
            title: 'Type',
            dataIndex: 'nativeDataType',
            key: 'nativeDataType',
            width: 300,
            render: (text?: string) => text || '—',
        },
        {
            title: 'Personal Data Types',
            key: 'current',
            width: 300,
            render: (_, record) => {
                const vals = annotationsMap.get(record.fieldPath) || [];
                if (vals.length === 0) {
                    return <Text type="secondary">Not set</Text>;
                }
                return (
                    <Space size={[0, 4]} wrap>
                        {vals.map((v) => (
                            <Tag key={v} color="blue">
                                {v}
                            </Tag>
                        ))}
                    </Space>
                );
            },
        },
        {
            title: 'Change To (multiple allowed)',
            key: 'changeTo',
            width: 300,
            render: (_, record) => {
                const current = annotationsMap.get(record.fieldPath) || [];
                const pending = pendingChanges[record.fieldPath];
                return (
                    <Select
                        mode="multiple"
                        value={pending !== undefined ? pending : current}
                        onChange={(vals) => handleChange(record.fieldPath, vals)}
                        style={{ width: '100%' }}
                        placeholder="Select Personal Data Types"
                        allowClear
                        showSearch
                    >
                        {getStructuredPropAllowedValues(piiAnnotationPropData).map((opt) => (
                            <Option key={opt} value={opt}>
                                {opt}
                            </Option>
                        ))}
                    </Select>
                );
            },
        },
    ];

    const selectedHasPersonalData =
        pendingHasPersonalDataChange !== undefined ? pendingHasPersonalDataChange : String(hasPersonalData ?? '');

    return (
        <Container>
            {!hasChanges && (
                <Button type="primary" onClick={onClose}>
                    Back
                </Button>
            )}

            <Descriptions bordered column={1}>
                <Descriptions.Item label="Has Personal Data?">
                    <Select
                        value={selectedHasPersonalData}
                        onChange={handleHasPersonalDataChange}
                        style={{ width: 100 }}
                        placeholder="Set Has Personal Data?"
                        size="small"
                    >
                        {getStructuredPropAllowedValues(hasPersonalDataPropData).map((opt) => (
                            <Option key={opt} value={opt}>
                                {opt}
                            </Option>
                        ))}
                    </Select>
                </Descriptions.Item>

                {isFieldAnnotationsVisible && (
                    <Descriptions.Item label="Field Annotations">
                        <Table
                            rowKey="fieldPath"
                            dataSource={fields}
                            columns={columns}
                            pagination={{ pageSize: 20 }}
                            bordered
                            size="middle"
                        />
                    </Descriptions.Item>
                )}
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
                        {pendingHasPersonalDataChange !== undefined &&
                            `You are about to change Has Personal Data to ${pendingHasPersonalDataChange}`}
                        <br />
                        You are about to save Personal Data Types for{' '}
                        <strong>{Object.keys(pendingChanges).length}</strong> column(s).
                        <br />
                        <br />
                        This will update multiple Personal Data Types per column where changed.
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
                        All Personal Data Type annotations will revert to their original values.
                        <br />
                        <br />
                        Are you sure?
                    </Text>
                )}
            </Modal>
        </Container>
    );
}

export default EditAnnotation;
