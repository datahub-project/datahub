import { Tooltip } from '@components';
import { Button, Form, Input, Space, Typography } from 'antd';
import React, { useEffect, useState } from 'react';
import styled from 'styled-components';

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
    isEmailEnabled: boolean;
    emailAddress?: string;
    onChange: (newAddress: string | undefined) => void;
}

export const EmailDefaults = ({ isEmailEnabled, emailAddress, onChange }: Props) => {
    const [editing, setEditing] = useState<boolean>(isEmailEnabled && !emailAddress?.length);
    const [inputValue, setInputValue] = useState<string | undefined>(emailAddress);

    useEffect(() => {
        setEditing(isEmailEnabled && !emailAddress?.trim()?.length);
    }, [emailAddress, isEmailEnabled]);

    useEffect(() => {
        setInputValue(emailAddress);
    }, [emailAddress]);

    const handleInputChange = (e: React.ChangeEvent<HTMLInputElement>) => {
        setInputValue(e.target.value);
    };

    const onSave = async () => {
        onChange(inputValue);
        setEditing(false);
    };

    return (
        <Form.Item name="default-email-address" label={<StyledLabel strong>Email Address</StyledLabel>}>
            <InputDiv>
                <Space direction="horizontal">
                    {!editing ? (
                        <>
                            <StyledLabel
                                disabled={!isEmailEnabled}
                                style={{ marginRight: '16px', display: 'inline-block' }}
                            >
                                {emailAddress || 'None'}
                            </StyledLabel>
                            <Tooltip
                                title={!isEmailEnabled ? 'Email notifications are currently disabled.' : undefined}
                            >
                                <Button
                                    size="small"
                                    disabled={!isEmailEnabled || editing}
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
                                placeholder="admin@your-company.com"
                                value={inputValue}
                                disabled={!isEmailEnabled}
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
                                disabled={!isEmailEnabled || !inputValue?.length}
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
