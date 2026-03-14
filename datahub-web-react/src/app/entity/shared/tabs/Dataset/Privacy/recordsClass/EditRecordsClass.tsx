import React, { useState } from 'react';
import { Button, Descriptions, message, Modal, Select, Space, Tag, Typography } from 'antd';
import styled from 'styled-components';

import {
    useRemoveStructuredPropertiesMutation,
    useUpsertStructuredPropertiesMutation,
} from '@graphql/structuredProperties.generated';

import { CompliancePropertyQualifiedName } from '@app/entity/shared/tabs/Dataset/Privacy/constant';
import {
    buildStructuredPropertyUrn,
    createRemoveMutation,
    createUpsertMutation,
    getStructuredList,
    getStructuredPropAllowedValues,
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

type EditRecordsClassProps = {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    structuredProps: any[];
    urn: string;
    onClose: () => void;
    refetchDataset: () => void;
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    recordsClassPropData: any;
};

export function EditRecordsClass({
    structuredProps,
    urn,
    onClose,
    refetchDataset,
    recordsClassPropData,
}: EditRecordsClassProps) {
    const [upsertProp, { loading: mutLoading }] = useUpsertStructuredPropertiesMutation();
    const [removeProp] = useRemoveStructuredPropertiesMutation();

    const currentRecordsClasses = getStructuredList(structuredProps, CompliancePropertyQualifiedName.RecordsClass);

    const [pendingValues, setPendingValues] = useState<string[] | undefined>(undefined);
    const [confirmAction, setConfirmAction] = useState<'save' | 'discard' | null>(null);

    const targetPropUrn = buildStructuredPropertyUrn(CompliancePropertyQualifiedName.RecordsClass);

    const selectedValues = pendingValues !== undefined ? pendingValues : currentRecordsClasses;

    const hasChanges = (() => {
        if (pendingValues === undefined) return false;
        if (pendingValues.length !== currentRecordsClasses.length) return true;
        const sortedPending = [...pendingValues].sort();
        const sortedCurrent = [...currentRecordsClasses].sort();
        return sortedPending.some((val, idx) => val !== sortedCurrent[idx]);
    })();

    const handleChange = (values: string[]) => {
        setPendingValues(values);
    };

    const handleSaveAll = async () => {
        const mutations: Promise<unknown>[] = [];

        if (pendingValues !== undefined) {
            if (pendingValues.length === 0) {
                mutations.push(createRemoveMutation(urn, targetPropUrn, removeProp));
            } else {
                mutations.push(createUpsertMutation(urn, [{ urn: targetPropUrn, values: pendingValues }], upsertProp));
            }
        }

        try {
            await Promise.all(mutations);
            message.success('Records Class updated successfully!');
            setPendingValues(undefined);
            refetchDataset();
            onClose();
        } catch (err: unknown) {
            const errorMessage = err instanceof Error ? err.message : 'Unknown error';
            message.error(`Save failed: ${errorMessage}`);
        }
    };

    const handleDiscardAll = () => {
        setPendingValues(undefined);
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

    return (
        <Container>
            {!hasChanges && (
                <Button type="primary" onClick={onClose}>
                    Back
                </Button>
            )}

            <Descriptions bordered column={1}>
                <Descriptions.Item label="Records Class">
                    <Space direction="vertical" style={{ width: '100%' }}>
                        <Space size={[0, 4]} wrap>
                            {currentRecordsClasses.length > 0 ? (
                                currentRecordsClasses.map((cls) => (
                                    <Tag key={cls} color="blue">
                                        {cls}
                                    </Tag>
                                ))
                            ) : (
                                <Text type="secondary">None currently set</Text>
                            )}
                        </Space>
                        <Select
                            mode="multiple"
                            value={selectedValues}
                            onChange={handleChange}
                            style={{ width: '100%' }}
                            placeholder="Select Records Classes"
                            allowClear
                            showSearch
                        >
                            {getStructuredPropAllowedValues(recordsClassPropData).map((opt) => (
                                <Option key={opt} value={opt}>
                                    {opt}
                                </Option>
                            ))}
                        </Select>
                    </Space>
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
                        You are about to update the Records Class to:{' '}
                        <strong>{pendingValues?.join(', ') || 'None'}</strong>.
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
                        Records Class will revert to its original value.
                        <br />
                        <br />
                        Are you sure?
                    </Text>
                )}
            </Modal>
        </Container>
    );
}

export default EditRecordsClass;
