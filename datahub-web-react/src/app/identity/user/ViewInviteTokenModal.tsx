import React, { useEffect, useMemo, useState } from 'react';
import * as QueryString from 'query-string';
import { useLocation } from 'react-router';
import { RedoOutlined, UserOutlined } from '@ant-design/icons';
import { Button, Col, message, Modal, Select, Typography } from 'antd';
import styled from 'styled-components';
import { PageRoutes } from '../../../conf/Global';
import { useGetInviteTokenQuery, useListRolesQuery } from '../../../graphql/role.generated';
import { DataHubRole } from '../../../types.generated';
import { mapRoleIcon } from './UserUtils';
import { useCreateInviteTokenMutation } from '../../../graphql/mutations.generated';

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

const InviteLinkParagraph = styled(Typography.Paragraph)`
    display: flex;
    flex-direction: row;
    justify-content: flex-start;
`;

const CreateInviteTokenButton = styled(Button)`
    display: inline-block;
    width: 20px;
    margin-left: -6px;
`;

const RoleSelect = styled(Select)`
    margin-top: 11px;
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
    const [isInviteTokenCreated, setIsInviteTokenCreated] = useState(false);
    const [createInviteTokenMutation, { data: createInviteTokenData }] = useCreateInviteTokenMutation();

    // Code related to listing role options and selecting a role
    const noRoleText = 'No Role';

    const { data: rolesData } = useListRolesQuery({
        fetchPolicy: 'no-cache',
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

    const onSelectRole = (roleUrn: string) => {
        const roleFromMap: DataHubRole = rolesMap.get(roleUrn) as DataHubRole;
        setSelectedRole(roleFromMap);
    };

    // Code related to creating an invite token
    const { data: getInviteTokenData } = useGetInviteTokenQuery({
        skip: !visible,
        variables: { input: { roleUrn: selectedRole?.urn } },
    });

    const createInviteToken = (roleUrn?: string) => {
        createInviteTokenMutation({
            variables: {
                input: {
                    roleUrn,
                },
            },
        })
            .then(({ errors }) => {
                if (!errors) {
                    setIsInviteTokenCreated(true);
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

    const inviteToken = useMemo(() => {
        if (isInviteTokenCreated) {
            return createInviteTokenData?.createInviteToken?.inviteToken;
        }
        return getInviteTokenData?.getInviteToken?.inviteToken || '';
    }, [getInviteTokenData, createInviteTokenData, isInviteTokenCreated]);

    const inviteLink = `${baseUrl}${PageRoutes.SIGN_UP}?invite_token=${inviteToken}`;

    return (
        <Modal
            width={1000}
            footer={null}
            title={
                <Typography.Text>
                    <b>Invite Users</b>
                </Typography.Text>
            }
            visible={visible}
            onCancel={onClose}
        >
            <ModalSection>
                <ModalSectionHeader strong>Role for users joining with this invite link</ModalSectionHeader>
                <InviteLinkParagraph>
                    <Col span={3}>
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
                    </Col>
                    <Col span={16}>
                        <Typography.Paragraph copyable={{ text: inviteLink }}>
                            <pre>{inviteLink}</pre>
                        </Typography.Paragraph>
                    </Col>
                </InviteLinkParagraph>
            </ModalSection>
            <ModalSection>
                <ModalSectionHeader strong>Generate a new link</ModalSectionHeader>
                <ModalSectionParagraph>
                    Generate a new invite link! Note, any old links will <b>cease to be active</b>.
                </ModalSectionParagraph>
                <CreateInviteTokenButton onClick={() => createInviteToken(selectedRole?.urn)} size="small" type="text">
                    <RedoOutlined style={{}} />
                </CreateInviteTokenButton>
            </ModalSection>
        </Modal>
    );
}
