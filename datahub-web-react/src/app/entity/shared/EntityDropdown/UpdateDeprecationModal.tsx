/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Button, DatePicker, Form, Modal, message } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { ANTD_GRAY } from '@app/entity/shared/constants';
import { Editor } from '@app/entity/shared/tabs/Documentation/components/editor/Editor';
import { handleBatchError } from '@app/entity/shared/utils';

import { useBatchUpdateDeprecationMutation } from '@graphql/mutations.generated';

type Props = {
    urns: string[];
    onClose: () => void;
    refetch?: () => void;
};

const StyledEditor = styled(Editor)`
    border: 1px solid ${ANTD_GRAY[4.5]};
`;

export const UpdateDeprecationModal = ({ urns, onClose, refetch }: Props) => {
    const [batchUpdateDeprecation] = useBatchUpdateDeprecationMutation();
    const [form] = Form.useForm();

    const handleClose = () => {
        form.resetFields();
        onClose();
    };

    const handleOk = async (formData: any) => {
        message.loading({ content: 'Updating...' });
        try {
            await batchUpdateDeprecation({
                variables: {
                    input: {
                        resources: [...urns.map((urn) => ({ resourceUrn: urn }))],
                        deprecated: true,
                        note: formData.note,
                        decommissionTime: formData.decommissionTime && formData.decommissionTime.unix() * 1000,
                    },
                },
            });
            message.destroy();
            message.success({ content: 'Deprecation Updated', duration: 2 });
        } catch (e: unknown) {
            message.destroy();
            if (e instanceof Error) {
                message.error(
                    handleBatchError(urns, e, {
                        content: `Failed to update Deprecation: \n ${e.message || ''}`,
                        duration: 2,
                    }),
                );
            }
        }
        refetch?.();
        handleClose();
    };

    return (
        <Modal
            title="Add Deprecation Details"
            open
            onCancel={handleClose}
            keyboard
            footer={
                <>
                    <Button onClick={handleClose} type="text">
                        Cancel
                    </Button>
                    <Button form="addDeprecationForm" key="submit" htmlType="submit">
                        Ok
                    </Button>
                </>
            }
            width="40%"
        >
            <Form form={form} name="addDeprecationForm" onFinish={handleOk} layout="vertical">
                <Form.Item name="note" label="Note" rules={[{ whitespace: true }]}>
                    <StyledEditor />
                </Form.Item>
                <Form.Item name="decommissionTime" label="Decommission Date">
                    <DatePicker style={{ width: '100%' }} />
                </Form.Item>
            </Form>
        </Modal>
    );
};
