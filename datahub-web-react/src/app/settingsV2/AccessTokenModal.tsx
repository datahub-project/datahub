import { Copy } from '@phosphor-icons/react/dist/csr/Copy';
import { Info } from '@phosphor-icons/react/dist/csr/Info';
import React from 'react';
import { Trans, useTranslation } from 'react-i18next';
import styled from 'styled-components/macro';

import { Button, Icon, Modal, Text, toast } from '@src/alchemy-components';
import { radius, spacing, typography } from '@src/alchemy-components/theme';
import { resolveRuntimePath } from '@utils/runtimeBasePath';

const ModalContent = styled.div`
    display: flex;
    flex-direction: column;
    gap: ${spacing.lg};
`;

const InfoAlert = styled.div`
    display: flex;
    align-items: center;
    gap: ${spacing.xsm};
    padding: ${spacing.sm} ${spacing.md};
    background: ${(props) => props.theme.colors.bgSurfaceInfo};
    border: 1px solid ${(props) => props.theme.colors.borderInformation};
    border-radius: ${radius.md};
    color: ${(props) => props.theme.colors.textInformation};
`;

const InfoIconWrapper = styled.div`
    display: flex;
    font-size: ${typography.fontSizes.lg};
    color: ${(props) => props.theme.colors.iconInformation};
`;

const Section = styled.div`
    display: flex;
    flex-direction: column;
    gap: ${spacing.xsm};
`;

const CodeBlock = styled.div`
    position: relative;
    background: ${(props) => props.theme.colors.bgCode};
    border: 1px solid ${(props) => props.theme.colors.border};
    border-radius: ${radius.md};
    padding: ${spacing.sm};
    overflow-x: auto;
`;

const CodeContent = styled.pre`
    margin: 0;
    white-space: pre-wrap;
    word-break: break-all;
    font-family: ${typography.fonts.mono};
    font-size: ${typography.fontSizes.sm};
    line-height: 1.5;
    color: ${(props) => props.theme.colors.textSecondary};
`;

const CopyButton = styled(Button)`
    position: absolute;
    top: ${spacing.xsm};
    right: ${spacing.xsm};
    background: ${(props) => props.theme.colors.bg};

    &:hover {
        background: ${(props) => props.theme.colors.bgHover};
    }
`;

const Kbd = styled.code`
    display: inline;
    padding: ${spacing.xxsm} ${spacing.xsm};
    background: ${(props) => props.theme.colors.bgCode};
    border: 1px solid ${(props) => props.theme.colors.border};
    border-radius: ${radius.sm};
    font-family: ${typography.fonts.mono};
    font-size: ${typography.fontSizes.sm};
`;

const ExpirationText = styled(Text)`
    color: ${(props) => props.theme.colors.textTertiary};
`;

const Link = styled.a`
    color: ${(props) => props.theme.colors.hyperlinks};
    text-decoration: none;

    &:hover {
        text-decoration: underline;
    }
`;

const ModalFooter = styled.div`
    display: flex;
    justify-content: flex-end;
`;

type Props = {
    visible: boolean;
    onClose: () => void;
    accessToken: string;
    expiresInText: string;
};

export const AccessTokenModal = ({ visible, onClose, accessToken, expiresInText }: Props) => {
    const { t } = useTranslation('settings.tokens');
    const { t: tc } = useTranslation('common.actions');
    const baseUrl = window.location.origin;
    const accessTokenCurl = `curl -X POST '${baseUrl}${resolveRuntimePath('/api/graphql')}' \\
--header 'Authorization: Bearer ${accessToken}' \\
--header 'Content-Type: application/json' \\
--data-raw '{"query":"{\\n  me {\\n    corpUser {\\n        username\\n    }\\n  }\\n}","variables":{}}'`;

    const copyToClipboard = (text: string, label: string) => {
        navigator.clipboard.writeText(text).then(
            () => toast.success(t('copiedToClipboard', { label })),
            () => toast.error(t('copyFailed', { label })),
        );
    };

    if (!visible) {
        return null;
    }

    return (
        <Modal
            width={700}
            title={t('newTokenTitle')}
            onCancel={onClose}
            dataTestId="access-token-modal"
            footer={
                <ModalFooter>
                    <Button id="createTokenButton" onClick={onClose} data-testid="access-token-modal-close-button">
                        {tc('close')}
                    </Button>
                </ModalFooter>
            }
        >
            <ModalContent>
                <InfoAlert>
                    <InfoIconWrapper>
                        <Icon icon={Info} size="inherit" />
                    </InfoIconWrapper>
                    <Text size="sm">{t('copyWarning')}</Text>
                </InfoAlert>

                <Section>
                    <Text size="md" weight="semiBold">
                        {t('tokenSectionTitle')}
                    </Text>
                    <ExpirationText size="sm">{expiresInText}</ExpirationText>
                    <CodeBlock>
                        <CodeContent data-testid="access-token-value">{accessToken}</CodeContent>
                        <CopyButton
                            variant="text"
                            size="sm"
                            onClick={() => copyToClipboard(accessToken, t('tokenCopyLabel'))}
                            data-testid="copy-token-button"
                            icon={{ icon: Copy }}
                        >
                            {tc('copy')}
                        </CopyButton>
                    </CodeBlock>
                </Section>

                <Section>
                    <Text size="md" weight="semiBold">
                        {t('usageSectionTitle')}
                    </Text>
                    <Text size="sm" color="gray">
                        <Trans t={t} i18nKey="tokenUsageDescription" components={{ kbd: <Kbd /> }} />
                    </Text>
                    <CodeBlock>
                        <CodeContent data-testid="access-token-curl">{accessTokenCurl}</CodeContent>
                        <CopyButton
                            variant="text"
                            size="sm"
                            onClick={() => copyToClipboard(accessTokenCurl, t('curlCopyLabel'))}
                            data-testid="copy-curl-button"
                            icon={{ icon: Copy }}
                        >
                            {tc('copy')}
                        </CopyButton>
                    </CodeBlock>
                </Section>

                <Section>
                    <Text size="md" weight="semiBold">
                        {t('learnMoreSectionTitle')}
                    </Text>
                    <Text size="sm" color="gray">
                        <Trans
                            t={t}
                            i18nKey="learnMoreDescription"
                            components={{
                                link: (
                                    <Link
                                        href="https://www.datahubproject.io/docs/"
                                        target="_blank"
                                        rel="noopener noreferrer"
                                    />
                                ),
                            }}
                        />
                    </Text>
                </Section>
            </ModalContent>
        </Modal>
    );
};
