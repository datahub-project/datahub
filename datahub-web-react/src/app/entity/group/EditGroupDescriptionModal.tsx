import React from 'react';
import { Button, Modal, Typography, Form } from 'antd';
import styled from 'styled-components';

import { Editor } from '../shared/tabs/Documentation/components/editor/Editor';
import { ANTD_GRAY } from '../shared/constants';

type Props = {
    onClose: () => void;
    onSaveAboutMe: () => void;
    setStagedDescription: (des: string) => void;
    stagedDescription: string | undefined;
};
const StyledEditor = styled(Editor)`
    border: 1px solid ${ANTD_GRAY[4]};
`;

export default function EditGroupDescriptionModal({
    onClose,
    onSaveAboutMe,
    setStagedDescription,
    stagedDescription,
}: Props) {
    const [form] = Form.useForm();

    function updateDescription(description: string) {
        setStagedDescription(description);
    }

    const saveDescription = () => {
        onSaveAboutMe();
        onClose();
    };
    return (
        <Modal
            width={700}
            title="Edit Description"
            visible
            onCancel={onClose}
            footer={
                <>
                    <Button onClick={onClose} type="text">
                        Cancel
                    </Button>
                    <Button id="createGroupButton" onClick={saveDescription}>
                        Update
                    </Button>
                </>
            }
        >
            <Form form={form} initialValues={{}} layout="vertical">
                <Form.Item label={<Typography.Text strong>Description</Typography.Text>}>
                    <Typography.Paragraph>An optional description for your new group.</Typography.Paragraph>
                    <Form.Item name="description" rules={[{ whitespace: true }, { min: 1, max: 500 }]} hasFeedback>
                        <StyledEditor content={stagedDescription} onChange={updateDescription} />
                    </Form.Item>
                </Form.Item>
            </Form>
        </Modal>
    );
}
