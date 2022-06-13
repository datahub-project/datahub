import React, { useState, useEffect } from 'react';
import styled from 'styled-components';
import { Typography, Modal, Button, Form, Input } from 'antd';
import { NotificationSink, PlatformNotificationOptions, SLACK_SINK } from './types';

type Props = {
    initialState?: PlatformNotificationOptions;
    sinks: Array<NotificationSink>;
    visible: boolean;
    onDone: (result: PlatformNotificationOptions) => void;
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
    slackChannel: undefined,
};

export const PlatformNotificationOptionsModal = ({ initialState, visible, sinks, onDone, onClose }: Props) => {
    const [options, setOptions] = useState<PlatformNotificationOptions>(initialState || DEFAULT_OPTIONS);

    useEffect(() => {
        setOptions(initialState || DEFAULT_OPTIONS);
    }, [initialState, setOptions]);

    const isSlackEnabled = sinks.some((sink) => sink.id === SLACK_SINK.id);

    return (
        <Modal
            width={400}
            title={<Typography.Text>Notification Options</Typography.Text>}
            visible={visible}
            onCancel={onClose}
            footer={
                <>
                    <Button onClick={onClose} type="text">
                        Cancel
                    </Button>
                    <Button id="updatePlatformNotificationOptionsButton" onClick={() => onDone(options)}>
                        Update
                    </Button>
                </>
            }
        >
            <Form layout="vertical">
                {isSlackEnabled && (
                    <Form.Item label={<Typography.Text strong>Slack channel</Typography.Text>}>
                        <Typography.Text type="secondary">
                            Enter a custom channel to notify. If not provided, the configured default will be used.
                        </Typography.Text>
                        <InputDiv>
                            <Input
                                value={options.slackChannel}
                                onChange={(e) => setOptions({ ...options, slackChannel: e.target.value })}
                                placeholder="#datahub-slack-notifications"
                            />
                        </InputDiv>
                    </Form.Item>
                )}
            </Form>
        </Modal>
    );
};
