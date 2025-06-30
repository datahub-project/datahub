import { Button, Form, Modal } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';

import { ANTD_GRAY } from '@app/entity/shared/constants';
import { Editor } from '@app/entity/shared/tabs/Documentation/components/editor/Editor';

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
    const [aboutText, setAboutText] = useState(stagedDescription);

    function updateDescription(description: string) {
        setAboutText(aboutText);
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
            open
            onCancel={onClose}
            footer={
                <>
                    <Button onClick={onClose} type="text">
                        Cancel
                    </Button>
                    <Button id="updateGroupButton" onClick={saveDescription} disabled={!stagedDescription}>
                        Update
                    </Button>
                </>
            }
        >
            <Form form={form} initialValues={{}} layout="vertical">
                <Form.Item name="description" rules={[{ whitespace: true }, { min: 1, max: 500 }]} hasFeedback>
                    <div>
                        <StyledEditor content={aboutText} onChange={updateDescription} />
                    </div>
                </Form.Item>
            </Form>
        </Modal>
    );
}
