import { RedoOutlined } from '@ant-design/icons';
import { Button, Modal, Typography, message } from 'antd';
import React, { useState } from 'react';
import { Trans, useTranslation } from 'react-i18next';
import styled from 'styled-components';

import analytics, { EventType } from '@app/analytics';
import { PageRoutes } from '@conf/Global';
import { resolveRuntimePath } from '@utils/runtimeBasePath';

import { useCreateNativeUserResetTokenMutation } from '@graphql/user.generated';

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

const CreateResetTokenButton = styled(Button)`
    display: inline-block;
    width: 20px;
    margin-left: -6px;
`;

type Props = {
    open: boolean;
    userUrn: string;
    username: string;
    onClose: () => void;
};

export default function ViewResetTokenModal({ open, userUrn, username, onClose }: Props) {
    const { t } = useTranslation('entity.identity');
    const baseUrl = window.location.origin;
    const [hasGeneratedResetToken, setHasGeneratedResetToken] = useState(false);

    const [createNativeUserResetTokenMutation, { data: createNativeUserResetTokenData }] =
        useCreateNativeUserResetTokenMutation({});

    const createNativeUserResetToken = () => {
        createNativeUserResetTokenMutation({
            variables: {
                input: {
                    userUrn,
                },
            },
        })
            .then(({ errors }) => {
                if (!errors) {
                    analytics.event({
                        type: EventType.CreateResetCredentialsLinkEvent,
                        userUrn,
                    });
                    setHasGeneratedResetToken(true);
                    message.success(t('resetToken.generateSuccess'));
                }
            })
            .catch((e) => {
                message.destroy();
                message.error({
                    content: t('resetToken.generateError', { error: e.message || '' }),
                    duration: 3,
                });
            });
    };

    const resetToken = createNativeUserResetTokenData?.createNativeUserResetToken?.resetToken || '';

    const inviteLink = `${baseUrl}${resolveRuntimePath(`${PageRoutes.RESET_CREDENTIALS}?reset_token=${resetToken}`)}`;

    return (
        <Modal
            width={700}
            footer={null}
            title={
                <Typography.Text>
                    <b>{t('resetToken.modalTitle')}</b>
                </Typography.Text>
            }
            open={open}
            onCancel={onClose}
        >
            {hasGeneratedResetToken ? (
                <ModalSection>
                    <ModalSectionHeader strong>{t('resetToken.shareLink.header')}</ModalSectionHeader>
                    <ModalSectionParagraph>
                        <Trans
                            t={t}
                            i18nKey="resetToken.shareLink.description"
                            values={{ username }}
                            components={{ b: <b /> }}
                        />
                    </ModalSectionParagraph>
                    <Typography.Paragraph copyable={{ text: inviteLink }}>
                        <pre>{inviteLink}</pre>
                    </Typography.Paragraph>
                </ModalSection>
            ) : (
                <ModalSection>
                    <ModalSectionHeader strong>{t('resetToken.newLinkRequired.header')}</ModalSectionHeader>
                    <ModalSectionParagraph>{t('resetToken.newLinkRequired.description')}</ModalSectionParagraph>
                </ModalSection>
            )}
            <ModalSection>
                <ModalSectionHeader strong>{t('resetToken.generateLink.header')}</ModalSectionHeader>
                <ModalSectionParagraph>
                    <Trans t={t} i18nKey="resetToken.generateLink.description" components={{ b: <b /> }} />
                </ModalSectionParagraph>
                <CreateResetTokenButton
                    onClick={createNativeUserResetToken}
                    size="small"
                    type="text"
                    data-testid="refreshButton"
                >
                    <RedoOutlined style={{}} />
                </CreateResetTokenButton>
            </ModalSection>
        </Modal>
    );
}
