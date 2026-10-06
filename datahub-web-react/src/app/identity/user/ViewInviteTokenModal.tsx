import { Button, Modal, Text, Tooltip } from '@components';
import { Typography, message } from 'antd';
import React, { useEffect, useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components/macro';

import analytics, { EventType } from '@app/analytics';
import SimpleSelectRole from '@app/identity/user/SimpleSelectRole';
import { PageRoutes } from '@conf/Global';
import { resolveRuntimePath } from '@utils/runtimeBasePath';

import { useCreateInviteTokenMutation } from '@graphql/mutations.generated';
import { useGetInviteTokenQuery } from '@graphql/role.generated';
import { DataHubRole } from '@types';

const ModalSection = styled.div`
    display: flex;
    flex-direction: column;
    padding-bottom: 12px;
`;

const ModalSectionFooter = styled(Text)`
    &&&& {
        padding: 0px;
        margin: 0px;
        margin-bottom: 4px;
    }
`;

const InviteLinkDiv = styled.div`
    margin-top: -12px;
    display: flex;
    flex-direction: row;
    justify-content: space-between;
    gap: 10px;
    align-items: center;
`;

const CopyText = styled(Typography.Text)`
    display: flex;
    gap: 10px;
    align-items: center;
    flex: 1;
`;

type Props = {
    open: boolean;
    onClose: () => void;
};

export default function ViewInviteTokenModal({ open, onClose }: Props) {
    const { t } = useTranslation('entity.identity');
    const { t: tc } = useTranslation('common.actions');
    const baseUrl = window.location.origin;
    const [selectedRole, setSelectedRole] = useState<DataHubRole>();

    // Code related to getting or creating an invite token
    const { data: getInviteTokenData } = useGetInviteTokenQuery({
        skip: !open,
        variables: { input: { roleUrn: selectedRole?.urn } },
    });

    const [inviteToken, setInviteToken] = useState<string>(getInviteTokenData?.getInviteToken?.inviteToken || '');

    const [createInviteTokenMutation] = useCreateInviteTokenMutation();

    useEffect(() => {
        if (getInviteTokenData?.getInviteToken?.inviteToken) {
            setInviteToken(getInviteTokenData.getInviteToken.inviteToken);
        }
    }, [getInviteTokenData]);

    const createInviteToken = (roleUrn?: string) => {
        createInviteTokenMutation({
            variables: {
                input: {
                    roleUrn,
                },
            },
        })
            .then(({ data, errors }) => {
                if (!errors) {
                    analytics.event({
                        type: EventType.CreateInviteLinkEvent,
                        roleUrn,
                    });
                    setInviteToken(data?.createInviteToken?.inviteToken || '');
                    message.success(t('inviteToken.generateSuccess'));
                }
            })
            .catch((e) => {
                message.destroy();
                message.error({
                    content: t('inviteToken.createError', { roleName: selectedRole?.name, error: e.message || '' }),
                    duration: 3,
                });
            });
    };

    const inviteLink = `${baseUrl}${resolveRuntimePath(`${PageRoutes.SIGN_UP}?invite_token=${inviteToken}`)}`;

    return (
        <Modal
            width={950}
            footer={null}
            buttons={[]}
            title={t('inviteToken.modalTitle')}
            open={open}
            onCancel={onClose}
        >
            <ModalSection>
                <InviteLinkDiv>
                    <SimpleSelectRole
                        selectedRole={selectedRole}
                        onRoleSelect={setSelectedRole}
                        placeholder={t('inviteToken.noRole')}
                        size="md"
                    />
                    <CopyText className="meticulous-ignore">
                        <pre className="meticulous-ignore">{inviteLink}</pre>
                    </CopyText>
                    <Tooltip title={t('inviteToken.copyTooltip')}>
                        <Button
                            onClick={() => {
                                navigator.clipboard.writeText(inviteLink);
                                message.success(t('inviteToken.copiedSuccess'));
                            }}
                        >
                            {tc('copy')}
                        </Button>
                    </Tooltip>
                    <Tooltip title={t('inviteToken.generateNewLinkTooltip')}>
                        <Button
                            variant="outline"
                            onClick={() => {
                                createInviteToken(selectedRole?.urn);
                            }}
                        >
                            {tc('refresh')}
                        </Button>
                    </Tooltip>
                </InviteLinkDiv>
                <ModalSectionFooter color="textSecondary">{t('inviteToken.footerText')}</ModalSectionFooter>
            </ModalSection>
        </Modal>
    );
}
