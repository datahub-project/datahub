import { RedoOutlined } from '@ant-design/icons';
import { Button, Modal, Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';
import { PageRoutes } from '../../../conf/Global';
import {
    useCreateNativeUserInviteTokenMutation,
    useGetNativeUserInviteTokenQuery,
} from '../../../graphql/user.generated';

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
    visible: boolean;
    onClose: () => void;
};

export default function ViewInviteTokenModal({ visible, onClose }: Props) {
    const baseUrl = window.location.origin;
    const { data: getNativeUserInviteTokenData } = useGetNativeUserInviteTokenQuery({ skip: !visible });

    const [createNativeUserInviteToken, { data: createNativeUserInviteTokenData }] =
        useCreateNativeUserInviteTokenMutation({});

    const inviteToken = createNativeUserInviteTokenData?.createNativeUserInviteToken?.inviteToken
        ? createNativeUserInviteTokenData?.createNativeUserInviteToken.inviteToken
        : getNativeUserInviteTokenData?.getNativeUserInviteToken?.inviteToken || '';

    const inviteLink = `${baseUrl}${PageRoutes.SIGN_UP}?invite_token=${inviteToken}`;

    return (
        <Modal
            width={700}
            footer={null}
            title={
                <Typography.Text>
                    <b>Invite new DataHub users</b>
                </Typography.Text>
            }
            visible={visible}
            onCancel={onClose}
        >
            <ModalSection>
                <ModalSectionHeader strong>Share invite link</ModalSectionHeader>
                <ModalSectionParagraph>
                    Share this invite link with other users in your workspace!
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
                <CreateInviteTokenButton onClick={() => createNativeUserInviteToken({})} size="small" type="text">
                    <RedoOutlined style={{}} />
                </CreateInviteTokenButton>
            </ModalSection>
        </Modal>
    );
}
