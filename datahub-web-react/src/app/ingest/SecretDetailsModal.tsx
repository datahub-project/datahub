import { Modal, Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';
import { Secret } from '../../types.generated';

const Section = styled.div`
    display: flex;
    flex-direction: column;
    padding-bottom: 12px;
`;

const SectionHeader = styled(Typography.Text)`
    &&&& {
        padding: 0px;
        margin: 0px;
        margin-bottom: 4px;
    }
`;

const SectionParagraph = styled(Typography.Paragraph)`
    &&&& {
        padding: 0px;
        margin: 0px;
    }
`;

type Props = {
    secret: Secret;
    visible: boolean;
    onClose: () => void;
};

export const SecretDetailsModal = ({ secret, visible, onClose }: Props) => {
    // TODO: We should add created by etc.
    // TODO: Add the length of the secret at least.
    return (
        <Modal
            width={800}
            footer={null}
            title={<Typography.Text>View Secret</Typography.Text>}
            visible={visible}
            onCancel={onClose}
        >
            <Section>
                <SectionHeader strong>Name</SectionHeader>
                <SectionParagraph>{secret.name}</SectionParagraph>
            </Section>
        </Modal>
    );
};
