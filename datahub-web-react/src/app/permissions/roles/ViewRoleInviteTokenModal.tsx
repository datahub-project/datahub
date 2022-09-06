import React, { useMemo, useState } from 'react';
import { RedoOutlined } from '@ant-design/icons';
import { Button, message, Modal, Typography } from 'antd';
import styled from 'styled-components';
import { useGetInviteTokenQuery } from '../../../graphql/role.generated';
import { useCreateInviteTokenMutation } from '../../../graphql/mutations.generated';
import { DataHubRole } from '../../../types.generated';
import { PageRoutes } from '../../../conf/Global';

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

const CreateInviteTokenButton = styled(Button)`
    display: inline-block;
    width: 20px;
    margin-left: -6px;
`;

type Props = {
    role: DataHubRole;
    visible: boolean;
    onClose: () => void;
};

export default function ViewRoleInviteTokenModal({ role, visible, onClose }: Props) {
    const baseUrl = window.location.origin;
    const { data: getInviteTokenData } = useGetInviteTokenQuery({
        skip: !role?.urn,
        variables: { input: { roleUrn: role?.urn } },
    });
    const [isInviteTokenCreated, setIsInviteTokenCreated] = useState(false);

    const [createInviteTokenMutation, { data: createInviteTokenData }] = useCreateInviteTokenMutation();

    const createInviteToken = (roleUrn: string) => {
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
                    content: `Failed to create Invite Token for role ${role?.name} : \n ${e.message || ''}`,
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

    const inviteLink = `${baseUrl}${PageRoutes.SIGN_UP}?invite_token=${inviteToken}&role=${role?.urn}`;

    return (
        <Modal
            width={1000}
            footer={null}
            title={
                <Typography.Text>
                    <b>Invite Users to become {role?.name}s</b>
                </Typography.Text>
            }
            visible={visible}
            onCancel={onClose}
        >
            <ModalSection>
                <ModalSectionHeader strong>
                    Share this link with other users in your workspace to assume the {role?.name} role
                </ModalSectionHeader>
                <ModalSectionParagraph>
                    If a user does not have a DataHub account, they will be prompted to create one and then will be
                    assigned to the {role?.name} role.
                </ModalSectionParagraph>
                <Typography.Paragraph copyable={{ text: inviteLink }}>
                    <pre>{inviteLink}</pre>
                </Typography.Paragraph>
            </ModalSection>
            <ModalSection>
                <ModalSectionHeader strong>Generate a new link</ModalSectionHeader>
                <ModalSectionParagraph>
                    Generate a new invite link! Note, any old links will <b>cease to be active</b>.
                </ModalSectionParagraph>
                <CreateInviteTokenButton size="small" type="text" onClick={() => createInviteToken(role.urn)}>
                    <RedoOutlined style={{}} />
                </CreateInviteTokenButton>
            </ModalSection>
        </Modal>
    );
}
