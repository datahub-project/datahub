import { RedoOutlined } from '@ant-design/icons';
import { Button, message, Modal, Typography } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';
import { useTranslation } from 'react-i18next';
import { PageRoutes } from '../../../conf/Global';
import { useCreateNativeUserResetTokenMutation } from '../../../graphql/user.generated';
import analytics, { EventType } from '../../analytics';

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
    const { t } = useTranslation();
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
                    message.success('Generated new link to reset credentials');
                }
            })
            .catch((e) => {
                message.destroy();
                message.error({
                    content: `Failed to create new link to reset credentials : \n ${e.message || ''}`,
                    duration: 3,
                });
            });
    };

    const resetToken = createNativeUserResetTokenData?.createNativeUserResetToken?.resetToken || '';

    const inviteLink = `${baseUrl}${PageRoutes.RESET_CREDENTIALS}?reset_token=${resetToken}`;

    return (
        <Modal
            width={700}
            footer={null}
            title={
                <Typography.Text>
                    <b>{t('authentification.resetUserPassword')}</b>
                </Typography.Text>
            }
            open={open}
            onCancel={onClose}
        >
            {hasGeneratedResetToken ? (
                <ModalSection>
                    <ModalSectionHeader strong>{t('authentification.shareResetLink')}</ModalSectionHeader>
                    <ModalSectionParagraph>
                        {t('authentification.shareResetLinkDescription', { username: username })}
                    </ModalSectionParagraph>
                    <Typography.Paragraph copyable={{ text: inviteLink }}>
                        <pre>{inviteLink}</pre>
                    </Typography.Paragraph>
                </ModalSection>
            ) : (
                <ModalSection>
                    <ModalSectionHeader strong>{t('authentification.newLinkMustBeGenerated')}</ModalSectionHeader>
                    <ModalSectionParagraph>
                        {t('authentification.newLinkMustBeGeneratedDescription')}
                    </ModalSectionParagraph>
                </ModalSection>
            )}
            <ModalSection>
                <ModalSectionHeader strong>{t('authentification.generateNewLink')}</ModalSectionHeader>
                <ModalSectionParagraph>
                    {t('authentification.generateNewLinkDescription')}
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
