import React, { ReactNode } from 'react';
import styled from 'styled-components/macro';
import { Typography, Switch, Button } from 'antd';

const SinkSettings = styled.div`
    margin-top: 12px;
    display: flex;
    flex-direction: row;
    align-items: start;
`;

const SinkTextContainer = styled.div`
    display: flex;
    flex-direction: column;
    padding-left: 14px;
`;

const SinkTitle = styled(Typography.Text)`
    font-family: 'Manrope', sans-serif;
    font-size: 16px;
    line-height: 24px;
    font-weight: 700;
`;

const SinkSubheadingTextContainer = styled.div`
    display: flex;
    flex-direction: row;
    justify-context: flex-start;
    align-items: center;
`;

const SinkDescription = styled(Typography.Text)`
    font-family: 'Manrope', sans-serif;
    font-size: 14px;
    line-height: 20px;
    font-weight: 500;
    max-width: 400px;
`;

const SinkEditButton = styled(Button)`
    margin-left: -6px;
`;

const EditDescription = styled(Typography.Text)`
    font-weight: 700;
    text-decoration: underline;
`;

type Props = {
    sinkTitle: string;
    sinkDescription: ReactNode;
    setShowEditSettings: (showEditSettings: boolean) => void;
};

/**
 * Notification sink settings section component
 */
export const SinkSettingsSection = ({ sinkTitle, sinkDescription, setShowEditSettings }: Props) => {
    return (
        <SinkSettings>
            <Switch />
            <SinkTextContainer>
                <SinkTitle>{sinkTitle}</SinkTitle>
                <SinkSubheadingTextContainer>
                    <SinkDescription>
                        {sinkDescription}
                        <SinkEditButton type="link" onClick={() => setShowEditSettings(true)}>
                            <EditDescription>edit</EditDescription>
                        </SinkEditButton>
                    </SinkDescription>
                </SinkSubheadingTextContainer>
            </SinkTextContainer>
        </SinkSettings>
    );
};
