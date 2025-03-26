import React, { useState, useEffect } from 'react';
import styled from 'styled-components';
import { Typography, Modal, Form, Input } from 'antd';
import { Button } from '@src/alchemy-components';
import { ModalButtonContainer } from '@src/app/shared/button/styledComponents';
import { EMAIL_SINK, NotificationSink, NotificationTypeOptions, SLACK_SINK } from './types';

type Props = {
    initialState?: NotificationTypeOptions;
    sinks: Array<NotificationSink>;
    visible: boolean;
    onDone: (result: NotificationTypeOptions) => void;
    onClose?: () => void;
};

const InputDiv = styled.div`
    padding-top: 12px;
    width: 360px;
`;

/**
 * Default notification options
 */
const DEFAULT_OPTIONS = {
    slackChannel: null,
    email: null,
};

export const NotificationTypeOptionsModal = ({ initialState, visible, sinks, onDone, onClose }: Props) => {
    const [options, setOptions] = useState<NotificationTypeOptions>(initialState || DEFAULT_OPTIONS);

    useEffect(() => {
        setOptions(initialState || DEFAULT_OPTIONS);
    }, [initialState, setOptions]);

    const isSlackEnabled = sinks.some((sink) => sink.id === SLACK_SINK.id);
    const isEmailEnabled = sinks.some((sink) => sink.id === EMAIL_SINK.id);

    return (
        <Modal
            width={400}
            title={<Typography.Text>Notification Options</Typography.Text>}
            visible={visible}
            onCancel={onClose}
            footer={
                <ModalButtonContainer>
                    <Button onClick={onClose} variant="text" color="gray">
                        Cancel
                    </Button>
                    <Button id="updatePlatformNotificationOptionsButton" onClick={() => onDone(options)}>
                        Update
                    </Button>
                </ModalButtonContainer>
            }
        >
            <Form layout="vertical">
                {isEmailEnabled && (
                    <Form.Item label={<Typography.Text strong>Email Address</Typography.Text>}>
                        <Typography.Text type="secondary">
                            Enter a custom email address to notify. If not provided, the configured default will be
                            used.
                        </Typography.Text>
                        <InputDiv>
                            <Input
                                value={options.email || undefined}
                                onChange={(e) => setOptions({ ...options, email: e.target.value || null })}
                                placeholder="admin@your-company.com"
                            />
                        </InputDiv>
                    </Form.Item>
                )}
                {isSlackEnabled && (
                    <Form.Item label={<Typography.Text strong>Slack Channel</Typography.Text>}>
                        <Typography.Text type="secondary">
                            Enter a custom channel to notify. If not provided, the configured default will be used.
                        </Typography.Text>
                        <InputDiv>
                            <Input
                                value={options.slackChannel || undefined}
                                onChange={(e) => setOptions({ ...options, slackChannel: e.target.value || null })}
                                placeholder="#datahub-slack-notifications"
                            />
                        </InputDiv>
                    </Form.Item>
                )}
            </Form>
        </Modal>
    );
};
