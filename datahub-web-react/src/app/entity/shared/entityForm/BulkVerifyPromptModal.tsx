import { ArrowRightOutlined } from '@ant-design/icons';
import { Button, Modal } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { FormView, useEntityFormContext } from '@app/entity/shared/entityForm/EntityFormContext';
import { pluralize } from '@app/shared/textUtil';

const Title = styled.div`
    font-size: 16px;
    font-weight: 600;
    margin: 16px 0;
`;

const TextContent = styled.div`
    font-size: 14px;
`;

interface Props {
    isVisible: boolean;
    closeModal: () => void;
}

export default function BulkVerifyPromptModal({ isVisible, closeModal }: Props) {
    const {
        form: { setFormView },
        counts: {
            verificationType: { verifyReady },
        },
        entity: { setSelectedEntities },
    } = useEntityFormContext();

    const goToBulkVerify = () => {
        setFormView(FormView.BULK_VERIFY);
        setSelectedEntities([]);
    };

    return (
        <Modal
            open={isVisible}
            onCancel={closeModal}
            title={null}
            width={600}
            footer={
                <>
                    <Button onClick={closeModal}>Cancel</Button>
                    <Button type="primary" onClick={goToBulkVerify}>
                        <ArrowRightOutlined /> Verify Responses for {verifyReady} {pluralize(verifyReady, 'Asset')}
                    </Button>
                </>
            }
        >
            <Title> Congratulations on completing all required responses for every asset!</Title>
            <TextContent>
                Now, for the final step, let&apos;s do a quick review of your hard work and verify your responses.
            </TextContent>
        </Modal>
    );
}
