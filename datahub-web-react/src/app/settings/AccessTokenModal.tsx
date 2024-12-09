import React from 'react';
import { Alert, Button, Modal, Typography } from 'antd';
import styled from 'styled-components';
import { InfoCircleOutlined } from '@ant-design/icons';
import { useTranslation } from 'react-i18next';

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
    open: boolean;
    onClose: () => void;
    accessToken: string;
    expiresInText: string;
};

export const AccessTokenModal = ({ open, onClose, accessToken, expiresInText }: Props) => {
    const { t } = useTranslation();
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
                    <b> {t('token.newPersonalAccessToken')}</b>
                </Typography.Text>
            }
            open={open}
            onCancel={onClose}
            footer={
                <>
                    <Button id="createTokenButton" onClick={onClose} data-testid="access-token-modal-close-button">
                        {t('common.close')}
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
                            {t('token.makeSureYouCopyYourAccessToken')}
                        </span>
                    }
                />
            </ModalSection>
            <ModalSection>
                <ModalSectionHeader strong>{t('common.token')}</ModalSectionHeader>
                <ModalSectionParagraph>{expiresInText}</ModalSectionParagraph>
                <Typography.Paragraph copyable={{ text: accessToken }}>
                    <pre data-testid="access-token-value">{accessToken}</pre>
                </Typography.Paragraph>
            </ModalSection>
            <ModalSection>
                <ModalSectionHeader strong>{t('common.usage')}</ModalSectionHeader>
                <ModalSectionParagraph>
                    To use the token, provide it as a <Typography.Text keyboard>Bearer</Typography.Text> token in the{' '}
                    <Typography.Text keyboard>Authorization</Typography.Text> header when making API requests:
                </ModalSectionParagraph>
                <Typography.Paragraph copyable={{ text: accessTokenCurl }}>
                    <pre>{accessTokenCurl}</pre>
                </Typography.Paragraph>
            </ModalSection>
            <ModalSection>
                <ModalSectionHeader strong>{t('common.learnMore')}</ModalSectionHeader>
                <ModalSectionParagraph>
                    {t('token.learnMoreLink')}
                    <a href="https://www.datahubproject.io/docs/"> {t('token.learnMoreLinkText')}</a>
                </ModalSectionParagraph>
            </ModalSection>
        </Modal>
    );
};
