/**
 * Legacy Slack configuration component - manual Member ID input.
 *
 * Used when requireSlackOAuthBinding = false (legacy/migration mode).
 * Users manually enter their Slack Member ID which is saved via GraphQL mutation.
 *
 * This preserves the original behavior from acryl-main branch.
 */
import { MoreOutlined } from '@ant-design/icons';
import { Button, colors } from '@components';
import { Form } from 'antd';
import React, { useEffect, useRef, useState } from 'react';
import styled from 'styled-components/macro';

import {
    CancelButton,
    SaveButton,
    SinkButtonsContainer,
    StyledFormItem,
    StyledInput,
} from '@app/settingsV2/personal/notifications/section/styledComponents';
import { SLACK_CONNECTION_URN } from '@app/settingsV2/slack/utils';
import { TestNotificationButton } from '@app/shared/notifications/TestNotificationButton';
import { SlackNotificationSettingsInput } from '@src/types.generated';

const HelperText = styled.div`
    color: ${colors.gray[1700]};
    margin-top: 6px;
    font-size: 12px;
`;

const CurrentValue = styled.div`
    display: flex;
    align-items: center;
    justify-content: start;
    gap: 12px;
    font-size: 14px;
    color: ${colors.gray[1700]};
`;

type Props = {
    userHandle: string | null | undefined;
    sinkEnabled: boolean;
    updateSinkSetting: (input?: SlackNotificationSettingsInput) => void;
};

/**
 * Legacy manual Slack Member ID input.
 * User types their Member ID and saves it via GraphQL.
 */
export const SlackLegacyManualInput: React.FC<Props> = ({ userHandle, sinkEnabled, updateSinkSetting }) => {
    const [editing, setIsEditing] = useState<boolean>(false);
    const [inputValue, setInputValue] = useState(userHandle || '');
    const [form] = Form.useForm();

    form.setFieldsValue({ slackFormValue: inputValue });

    useEffect(() => {
        setInputValue(userHandle || '');
    }, [userHandle]);

    // Auto-open edit mode when sink is enabled but no ID is set
    const hasSetEditingRef = useRef(false);
    const hasComponentMountedRef = useRef(false);
    useEffect(() => {
        if (!hasComponentMountedRef.current) {
            hasComponentMountedRef.current = true;
            return;
        }
        if (!hasSetEditingRef.current) {
            setIsEditing(!userHandle && sinkEnabled);
            hasSetEditingRef.current = true;
        }
    }, [userHandle, sinkEnabled]);

    const saveSettings = () => {
        const input: SlackNotificationSettingsInput = { userHandle: inputValue };
        updateSinkSetting(input);
    };

    const saveButtonOnClick = () => {
        saveSettings();
        setIsEditing(false);
    };

    const cancelButtonOnClick = () => {
        setInputValue(userHandle || '');
        setIsEditing(false);
    };

    return (
        <>
            {!editing && (
                <CurrentValue>{inputValue ? <strong>{inputValue}</strong> : 'No Slack Member ID set.'}</CurrentValue>
            )}
            {!editing && (
                <Button variant="text" onClick={() => setIsEditing(true)} data-testid="slack-edit-member-id-button">
                    Edit
                </Button>
            )}
            {editing && (
                <>
                    <SinkButtonsContainer>
                        <Form form={form}>
                            <StyledFormItem name="slackFormValue">
                                <StyledInput
                                    placeholder="Slack Member ID"
                                    value={inputValue}
                                    onChange={(e) => setInputValue(e.target.value)}
                                />
                            </StyledFormItem>
                        </Form>
                        <SaveButton onClick={saveButtonOnClick} data-testid="slack-save-member-id-button">
                            Save
                        </SaveButton>
                        <CancelButton
                            variant="outline"
                            color="gray"
                            onClick={cancelButtonOnClick}
                            data-testid="slack-cancel-member-id-button"
                        >
                            Cancel
                        </CancelButton>
                    </SinkButtonsContainer>
                    <HelperText>
                        Find a member ID from the <MoreOutlined /> menu in your Slack profile.
                        <a
                            target="_blank"
                            rel="noreferrer"
                            href="https://datahubproject.io/docs/managed-datahub/slack/saas-slack-setup/#how-to-find-user-id-in-slack"
                        >
                            {' '}
                            See instructions.
                        </a>
                    </HelperText>
                </>
            )}
            {inputValue && (
                <TestNotificationButton
                    integration="slack"
                    connectionUrn={SLACK_CONNECTION_URN}
                    destinationSettings={{ userHandle: inputValue }}
                />
            )}
        </>
    );
};
