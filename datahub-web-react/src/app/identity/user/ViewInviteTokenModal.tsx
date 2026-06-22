import { UserOutlined } from '@ant-design/icons';
import { Button, Modal, Tooltip } from '@components';
import { Select, Typography, message } from 'antd';
import React, { useEffect, useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components/macro';

import analytics, { EventType } from '@app/analytics';
import { mapRoleIcon } from '@app/identity/user/UserUtils';
import { useRoleSelector } from '@app/identity/user/useRoleSelector';
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

const ModalSectionFooter = styled(Typography.Paragraph)`
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

const ActionsContainer = styled.div`
    display: flex;
    gap: 10px;
`;

const InfoContainer = styled.div`
    display: flex;
    align-items: center;
    gap: 10px;
`;

const CopyText = styled(Typography.Text)`
    display: flex;
    gap: 10px;
    align-items: center;
    flex: 1;
`;

const RoleSelect = styled(Select)`
    min-width: 105px;
`;

const RoleIcon = styled.span`
    margin-right: 6px;
    font-size: 12px;
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

    const noRoleText = t('inviteToken.noRole');

    const { roles: selectRoleOptions } = useRoleSelector();

    const rolesMap: Map<string, DataHubRole> = new Map();
    selectRoleOptions.forEach((role) => {
        rolesMap.set(role.urn, role);
    });

    const roleSelectOptions = () =>
        selectRoleOptions.map((role) => {
            return (
                <Select.Option key={role.urn} value={role.urn}>
                    <RoleIcon>{mapRoleIcon(role.name)}</RoleIcon>
                    {role.name}
                </Select.Option>
            );
        });

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

    const onSelectRole = (roleUrn: string) => {
        const roleFromMap: DataHubRole = rolesMap.get(roleUrn) as DataHubRole;
        setSelectedRole(roleFromMap);
    };

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
                    <InfoContainer>
                        <RoleSelect
                            placeholder={
                                <>
                                    <UserOutlined style={{ marginRight: 6, fontSize: 12 }} />
                                    {noRoleText}
                                </>
                            }
                            value={selectedRole?.urn || undefined}
                            onChange={(e) => onSelectRole(e as string)}
                        >
                            <Select.Option value="">
                                <RoleIcon>{mapRoleIcon(noRoleText)}</RoleIcon>
                                {noRoleText}
                            </Select.Option>
                            {roleSelectOptions()}
                        </RoleSelect>
                        <CopyText>
                            <pre className="meticulous-ignore">{inviteLink}</pre>
                        </CopyText>
                    </InfoContainer>
                    <ActionsContainer>
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
                    </ActionsContainer>
                </InviteLinkDiv>
                <ModalSectionFooter type="secondary">{t('inviteToken.footerText')}</ModalSectionFooter>
            </ModalSection>
        </Modal>
    );
}
