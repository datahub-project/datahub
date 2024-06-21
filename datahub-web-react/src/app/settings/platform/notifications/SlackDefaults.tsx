import React, { useState, useEffect } from 'react';

import styled from 'styled-components';
import { Typography, Form, Input, Button, Space, Tooltip } from 'antd';

const InputDiv = styled.div`
    width: 360px;
`;

const StyledInput = styled(Input)`
    max-width: 271px;
    font-size: 14px;
    .ant-input {
        font-size: 14px;
    }
`;

const StyledLabel = styled(Typography.Text)`
    display: inline-block;
    font-size: 14px;
`;

const StyledButton = styled(Button)`
    background: #00615f;
`;

interface Props {
    isSlackEnabled?: boolean;
    channel?: string;
    onChange: (newChannel: string | undefined) => void;
}

export const SlackDefaults = ({ isSlackEnabled = false, channel, onChange }: Props) => {
    const [editing, setEditing] = useState<boolean>(isSlackEnabled && !channel?.length);
    const [inputValue, setInputValue] = useState<string | undefined>(channel);

    useEffect(() => {
        setEditing(isSlackEnabled && !channel?.trim()?.length);
    }, [channel, isSlackEnabled]);

    useEffect(() => {
        setInputValue(channel);
    }, [channel]);

    const handleInputChange = (e: React.ChangeEvent<HTMLInputElement>) => {
        setInputValue(e.target.value);
    };

    const onSave = async () => {
        if (inputValue) {
            onChange(inputValue);
            setEditing(false);
        }
    };

    return (
        <Form.Item name="default-slack-channel" label={<StyledLabel strong>Slack Channel</StyledLabel>}>
            <InputDiv>
                <Space direction="horizontal">
                    {!editing ? (
                        <>
                            <StyledLabel
                                disabled={!isSlackEnabled}
                                style={{ marginRight: '16px', display: 'inline-block' }}
                            >
                                {channel ? `#${channel}` : 'None'}
                            </StyledLabel>
                            <Tooltip
                                title={!isSlackEnabled ? 'Slack notifications are currently disabled.' : undefined}
                            >
                                <Button
                                    size="small"
                                    disabled={!isSlackEnabled || editing}
                                    type="default"
                                    onClick={() => setEditing(true)}
                                >
                                    Edit
                                </Button>
                            </Tooltip>
                        </>
                    ) : (
                        <>
                            <StyledInput
                                placeholder="data-notifications"
                                addonBefore="#"
                                value={inputValue}
                                disabled={!isSlackEnabled}
                                onChange={handleInputChange}
                                onKeyPress={(e) => {
                                    if (e.key === 'Enter') {
                                        onSave();
                                    }
                                }}
                            />
                            <StyledButton
                                type="primary"
                                onClick={onSave}
                                disabled={!isSlackEnabled || !inputValue?.length}
                            >
                                Save
                            </StyledButton>
                        </>
                    )}
                </Space>
            </InputDiv>
        </Form.Item>
    );
};
