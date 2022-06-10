import React from 'react';
import { Alert, Button, Modal, Typography } from 'antd';
import styled from 'styled-components';
import { InfoCircleOutlined } from '@ant-design/icons';

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

const StyledAlert = styled(Alert)`
    padding-top: 12px;
    padding-bottom: 12px;
    margin-bottom: 20px;
`;

const StyledInfoCircleOutlined = styled(InfoCircleOutlined)`
    margin-right: 8px;
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
            title={
                <Typography.Text>
                    <b> New Personal Access Token</b>
                </Typography.Text>
            }
            visible={visible}
            onCancel={onClose}
            footer={
                <>
                    <Button id="createTokenButton" onClick={onClose}>
                        Close
                    </Button>
                </>
            }
        >
            <ModalSection>
                <StyledAlert
                    type="info"
                    message={
                        <span>
                            <StyledInfoCircleOutlined />
                            Make sure to copy your personal access token now. You won’t be able to see it again.
                        </span>
                    }
                />
            </ModalSection>
            <ModalSection>
                <ModalSectionHeader strong>Token</ModalSectionHeader>
                <ModalSectionParagraph>{expiresInText}</ModalSectionParagraph>
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
