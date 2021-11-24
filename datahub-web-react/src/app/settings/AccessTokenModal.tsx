import { Modal, Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';

const ModalSection = styled.div`
    display: flex;
    flex-direction: column;
    padding-bottom: 12px;
`;

const ModalSectionHeader = styled(Typography.Text)`
    &&&& {
        padding: 0px;
        margin: 0px;
        margin-bottom: 4px;
    }
`;

const ModalSectionParagraph = styled(Typography.Paragraph)`
    &&&& {
        padding: 0px;
        margin: 0px;
    }
`;

type Props = {
    visible: boolean;
    onClose: () => void;
    accessToken: string;
    expiresInText: string;
};

export const AccessTokenModal = ({ visible, onClose, accessToken, expiresInText }: Props) => {
    const baseUrl = window.location.origin;
    const accessTokenCurl = `curl -X POST '${baseUrl}/api/graphql' \\
--header 'Authorization: Bearer ${accessToken}' \\
--header 'Content-Type: application/json' \\
--data-raw '{"query":"{\\n  me {\\n    corpUser {\\n        username\\n    }\\n  }\\n}","variables":{}}'`;

    return (
        <Modal
            width={700}
            footer={null}
            title={
                <Typography.Text>
                    Generate a new <b>Personal Access Token</b>
                </Typography.Text>
            }
            visible={visible}
            onCancel={onClose}
        >
            <ModalSection>
                <ModalSectionHeader strong>Token</ModalSectionHeader>
                <ModalSectionParagraph>
                    This token will expire in <b>{expiresInText}.</b>
                </ModalSectionParagraph>
                <Typography.Paragraph copyable={{ text: accessToken }}>
                    <pre>{accessToken}</pre>
                </Typography.Paragraph>
            </ModalSection>
            <ModalSection>
                <ModalSectionHeader strong>Usage</ModalSectionHeader>
                <ModalSectionParagraph>
                    To use the token, provide it as a <Typography.Text keyboard>Bearer</Typography.Text> token in the{' '}
                    <Typography.Text keyboard>Authorization</Typography.Text> header when making API requests:
                </ModalSectionParagraph>
                <Typography.Paragraph copyable={{ text: accessTokenCurl }}>
                    <pre>{accessTokenCurl}</pre>
                </Typography.Paragraph>
            </ModalSection>
            <ModalSection>
                <ModalSectionHeader strong>Learn More</ModalSectionHeader>
                <ModalSectionParagraph>
                    To learn more about the DataHub APIs, check out the
                    <a href="https://www.datahubproject.io/docs/"> DataHub Docs.</a>
                </ModalSectionParagraph>
            </ModalSection>
        </Modal>
    );
};
