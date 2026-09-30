import { Button, Modal, Text, toast } from '@components';
import { ArrowClockwise } from '@phosphor-icons/react/dist/csr/ArrowClockwise';
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

const ModalSectionHeader = styled(Text)`
    &&&& {
        padding: 0px;
        margin: 0px;
        margin-bottom: 4px;
    }
`;

const ModalSectionParagraph = styled(Text)`
    &&&& {
        padding: 0px;
        margin: 0px;
    }
`;

const CreateResetTokenButton = styled(Button)`
    display: inline-flex;
    width: auto;
    margin-left: -6px;
`;

const InviteLinkRow = styled.div`
    display: flex;
    align-items: flex-start;
    gap: 8px;
`;

type Props = {
    open: boolean;
    userUrn: string;
    username: string;
    onClose: () => void;
};

export default function ViewResetTokenModal({ open, userUrn, username, onClose }: Props) {
    const { t } = useTranslation('entity.identity');
    const { t: tc } = useTranslation('common.actions');
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
                    toast.success(t('resetToken.generateSuccess'));
                }
            })
            .catch((e) => {
                toast.destroy();
                toast.error(t('resetToken.generateError', { error: e.message || '' }), { duration: 3 });
            });
    };

    const resetToken = createNativeUserResetTokenData?.createNativeUserResetToken?.resetToken || '';

    const inviteLink = `${baseUrl}${resolveRuntimePath(`${PageRoutes.RESET_CREDENTIALS}?reset_token=${resetToken}`)}`;

    return (
        <Modal width={700} buttons={[]} title={t('resetToken.modalTitle')} open={open} onCancel={onClose}>
            {hasGeneratedResetToken ? (
                <ModalSection>
                    <ModalSectionHeader weight="semiBold">{t('resetToken.shareLink.header')}</ModalSectionHeader>
                    <ModalSectionParagraph>
                        <Trans
                            t={t}
                            i18nKey="resetToken.shareLink.description"
                            values={{ username }}
                            components={{ bold: <b /> }}
                        />
                    </ModalSectionParagraph>
                    <InviteLinkRow>
                        <Text type="pre">{inviteLink}</Text>
                        <Button
                            variant="text"
                            size="sm"
                            onClick={() => {
                                navigator.clipboard.writeText(inviteLink);
                                toast.success(t('inviteToken.copiedSuccess'));
                            }}
                        >
                            {tc('copy')}
                        </Button>
                    </InviteLinkRow>
                </ModalSection>
            ) : (
                <ModalSection>
                    <ModalSectionHeader weight="semiBold">{t('resetToken.newLinkRequired.header')}</ModalSectionHeader>
                    <ModalSectionParagraph>{t('resetToken.newLinkRequired.description')}</ModalSectionParagraph>
                </ModalSection>
            )}
            <ModalSection>
                <ModalSectionHeader weight="semiBold">{t('resetToken.generateLink.header')}</ModalSectionHeader>
                <ModalSectionParagraph>
                    <Trans t={t} i18nKey="resetToken.generateLink.description" components={{ bold: <b /> }} />
                </ModalSectionParagraph>
                <CreateResetTokenButton
                    onClick={createNativeUserResetToken}
                    size="sm"
                    variant="text"
                    data-testid="refreshButton"
                    icon={{ icon: ArrowClockwise }}
                />
            </ModalSection>
        </Modal>
    );
}
