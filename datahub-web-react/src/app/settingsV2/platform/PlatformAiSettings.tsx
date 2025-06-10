import { InfoCircleFilled } from '@ant-design/icons';
import { Spin, message } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';

import { useGlobalSettingsContext } from '@app/context/GlobalSettings/GlobalSettingsContext';
import { PageTitle, Switch, colors } from '@src/alchemy-components';

import { useUpdateGlobalDocsAiSettingsMutation } from '@graphql/settings.generated';

const Container = styled.div`
    width: 100%;
    overflow: auto;
    padding: 16px 20px;
`;

const GlobalAIBanner = styled.div`
    background: #f9f0ff;
    border-radius: 8px;
    border: 1px solid ${colors.violet['500']};
    padding: 8px 12px;
    margin: 18px 0 25px;
    font-size: 14px;
    display: flex;
    flex-direction: column;
    gap: 4px;
`;

const InfoIcon = styled(InfoCircleFilled)`
    color: ${colors.violet['500']};
    margin-right: 8px;
`;

const DocsLink = styled.a`
    color: ${colors.violet['600']};
    margin-top: 4px;
    width: fit-content;
    &:hover {
        color: ${colors.violet['800']};
    }
`;

const StyledCard = styled.div`
    border: 1px solid ${colors.gray[100]};
    border-radius: 12px;
    box-shadow: 0px 1px 2px 0px rgba(33, 23, 95, 0.07);
    padding: 16px;
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 24px;
`;

const TextContainer = styled.div`
    display: flex;
    flex-direction: column;
    flex: 1;
`;

const SettingText = styled.div`
    font-size: 16px;
    color: ${colors.gray[600]};
    font-weight: 700;
`;

const DescriptionText = styled.div`
    color: ${colors.gray[1700]};
    font-size: 14px;
    font-weight: 400;
    line-height: 1.5;
`;

export const PlatformAiSettings = () => {
    const { globalSettings, refetch, loading } = useGlobalSettingsContext();
    const [updateGlobalDocsAiSettings, { loading: updating }] = useUpdateGlobalDocsAiSettingsMutation();
    const aiEnabled = globalSettings?.documentationAi?.enabled ?? false;
    const [switchValue, setSwitchValue] = useState(aiEnabled);
    const [localLoading, setLocalLoading] = useState(false);

    React.useEffect(() => {
        setSwitchValue(aiEnabled);
    }, [aiEnabled]);

    const handleToggle = async (checked: boolean) => {
        setLocalLoading(true);
        try {
            const res = await updateGlobalDocsAiSettings({
                variables: {
                    input: { enabled: checked },
                },
            });
            if (res.data?.updateGlobalSettings) {
                message.success(`AI documentation generation ${checked ? 'enabled' : 'disabled'}`);
                refetch();
            } else {
                message.error('Failed to update AI documentation setting');
            }
        } catch (e: any) {
            message.error('Failed to update AI documentation setting');
        } finally {
            setLocalLoading(false);
        }
    };

    let content: React.ReactNode = null;
    if (loading) {
        content = <Spin style={{ marginTop: 40 }} />;
    } else {
        content = (
            <StyledCard>
                <TextContainer>
                    <SettingText>Enable AI Documentation Generation</SettingText>
                    <DescriptionText>
                        When enabled, DataHub can generate documentation using AI for supported entities.
                    </DescriptionText>
                    <DocsLink
                        href="https://docs.datahub.com/docs/automations/ai-docs"
                        target="_blank"
                        rel="noopener noreferrer"
                    >
                        Learn more about AI-powered documentation.
                    </DocsLink>
                </TextContainer>
                <Switch
                    label=""
                    isChecked={switchValue}
                    isDisabled={localLoading || updating}
                    onChange={(e) => handleToggle(e.target.checked)}
                    data-testid="ai-docs-toggle"
                />
            </StyledCard>
        );
    }

    return (
        <Container>
            <PageTitle title="AI" subTitle="Configure AI-powered features" />
            <GlobalAIBanner>
                <span>
                    <InfoIcon />
                    These settings are global and affect all users.
                </span>
            </GlobalAIBanner>
            {content}
        </Container>
    );
};
