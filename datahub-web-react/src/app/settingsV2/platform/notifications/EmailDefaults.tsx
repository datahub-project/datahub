import React, { useState, useEffect } from 'react';

import styled from 'styled-components';
import { Input, Space } from 'antd';
import { Button, Card, Tooltip } from '@components';

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

const StyledLabel = styled.div`
    font-size: 14px;
`;

const StyledButton = styled(Button)``;

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
        <Card title="Email Notifications" subTitle="Receive Email notifications when important changes occur.">
            <InputDiv>
                <Space direction="horizontal">
                    {!editing ? (
                        <>
                            <StyledLabel style={{ marginRight: '12px' }}>{emailAddress || 'None'}</StyledLabel>
                            <Tooltip
                                title={!isEmailEnabled ? 'Email notifications are currently disabled.' : undefined}
                            >
                                <Button
                                    variant="text"
                                    disabled={!isEmailEnabled || editing}
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
                            <StyledButton onClick={onSave} disabled={!isEmailEnabled || !inputValue?.length}>
                                Save
                            </StyledButton>
                        </>
                    )}
                </Space>
            </InputDiv>
        </Card>
    );
};
