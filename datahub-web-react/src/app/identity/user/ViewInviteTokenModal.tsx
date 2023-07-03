import React, { useEffect, useState } from 'react';
import * as QueryString from 'query-string';
import { useLocation } from 'react-router';
import { UserOutlined } from '@ant-design/icons';
import { Button, message, Modal, Select, Tooltip, Typography } from 'antd';
import styled from 'styled-components/macro';
import { PageRoutes } from '../../../conf/Global';
import { useGetInviteTokenQuery, useListRolesQuery } from '../../../graphql/role.generated';
import { DataHubRole } from '../../../types.generated';
import { mapRoleIcon } from './UserUtils';
import { useCreateInviteTokenMutation } from '../../../graphql/mutations.generated';
import { ANTD_GRAY } from '../../entity/shared/constants';
import analytics, { EventType } from '../../analytics';

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
    justify-content: flex-start;
    gap: 10px;
    align-items: center;
`;

const CopyText = styled(Typography.Text)`
    display: flex;
    gap: 10px;
    align-items: center;
`;

const CopyButton = styled(Button)`
    background-color: #1890ff;
    color: white;

    &:focus:not(:hover) {
        background-color: #1890ff;
        color: white;
    }
`;

const RefreshButton = styled(Button)`
    color: ${ANTD_GRAY[7]};
`;

const RoleSelect = styled(Select)`
    min-width: 105px;
`;

const RoleIcon = styled.span`
    margin-right: 6px;
    font-size: 12px;
`;

type Props = {
    visible: boolean;
    onClose: () => void;
};

export default function ViewInviteTokenModal({ visible, onClose }: Props) {
    const baseUrl = window.location.origin;
    const location = useLocation();
    const params = QueryString.parse(location.search, { arrayFormat: 'comma' });
    const paramsQuery = (params?.query as string) || undefined;
    const [query, setQuery] = useState<undefined | string>(undefined);
    useEffect(() => setQuery(paramsQuery), [paramsQuery]);
    const [selectedRole, setSelectedRole] = useState<DataHubRole>();

    // Code related to listing role options and selecting a role
    const noRoleText = 'No Role';

    const { data: rolesData } = useListRolesQuery({
        fetchPolicy: 'cache-first',
        variables: {
            input: {
                start: 0,
                count: 10,
                query,
            },
        },
    });
    const selectRoleOptions = rolesData?.listRoles?.roles?.map((role) => role as DataHubRole) || [];

    const rolesMap: Map<string, DataHubRole> = new Map();
    selectRoleOptions.forEach((role) => {
        rolesMap.set(role.urn, role);
    });

    const roleSelectOptions = () =>
        selectRoleOptions.map((role) => {
            return (
                <Select.Option value={role.urn}>
                    <RoleIcon>{mapRoleIcon(role.name)}</RoleIcon>
                    {role.name}
                </Select.Option>
            );
        });

    // Code related to getting or creating an invite token
    const { data: getInviteTokenData } = useGetInviteTokenQuery({
        skip: !visible,
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
                    message.success('Generated new invite link');
                }
            })
            .catch((e) => {
                message.destroy();
                message.error({
                    content: `Failed to create Invite Token for role ${selectedRole?.name} : \n ${e.message || ''}`,
                    duration: 3,
                });
            });
    };

    const inviteLink = `${baseUrl}${PageRoutes.SIGN_UP}?invite_token=${inviteToken}`;

    return (
        <Modal
            width={950}
            footer={null}
            title={
                <Typography.Text>
                    <b>Share Invite Link</b>
                </Typography.Text>
            }
            visible={visible}
            onCancel={onClose}
        >
            <ModalSection>
                <InviteLinkDiv>
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
                        <pre>{inviteLink}</pre>
                    </CopyText>
                    <Tooltip title="Copy invite link.">
                        <CopyButton
                            onClick={() => {
                                navigator.clipboard.writeText(inviteLink);
                                message.success('Copied invite link to clipboard');
                            }}
                        >
                            COPY
                        </CopyButton>
                    </Tooltip>
                    <Tooltip title="Generate a new link. Any old links will no longer be valid.">
                        <RefreshButton
                            onClick={() => {
                                createInviteToken(selectedRole?.urn);
                            }}
                        >
                            REFRESH
                        </RefreshButton>
                    </Tooltip>
                </InviteLinkDiv>
                <ModalSectionFooter type="secondary">
                    Copy an invite link to send to your users. When they join, users will be automatically assigned to
                    the selected role.
                </ModalSectionFooter>
            </ModalSection>
        </Modal>
    );
}
