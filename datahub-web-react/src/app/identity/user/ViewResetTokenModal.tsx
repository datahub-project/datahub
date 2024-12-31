import { RedoOutlined } from '@ant-design/icons';
import { Button, message, Modal, Typography } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';
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
                    <b>Reset User Password</b>
                </Typography.Text>
            }
            open={open}
            onCancel={onClose}
        >
            {hasGeneratedResetToken ? (
                <ModalSection>
                    <ModalSectionHeader strong>Share reset link</ModalSectionHeader>
                    <ModalSectionParagraph>
                        Share this reset link to reset the credentials for {username}.
                        <b>This link will expire in 24 hours.</b>
                    </ModalSectionParagraph>
                    <Typography.Paragraph copyable={{ text: inviteLink }}>
                        <pre>{inviteLink}</pre>
                    </Typography.Paragraph>
                </ModalSection>
            ) : (
                <ModalSection>
                    <ModalSectionHeader strong>A new link must be generated</ModalSectionHeader>
                    <ModalSectionParagraph>
                        You cannot view any old reset links. Please generate a new one below.
                    </ModalSectionParagraph>
                </ModalSection>
            )}
            <ModalSection>
                <ModalSectionHeader strong>Generate a new link</ModalSectionHeader>
                <ModalSectionParagraph>
                    Generate a new reset link! Note, any old links will <b>cease to be active</b>.
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
