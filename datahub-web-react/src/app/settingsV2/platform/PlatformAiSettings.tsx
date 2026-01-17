import { InfoCircleFilled } from '@ant-design/icons';
import { Spin, message } from 'antd';
import React, { useState } from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

import { useGlobalSettingsContext } from '@app/context/GlobalSettings/GlobalSettingsContext';
import { AiPluginsSettings } from '@app/settingsV2/platform/aiPlugins';
import { SLACK_CONNECTION_URN } from '@app/settingsV2/platform/slack/constants';
import { decodeSlackConnection } from '@app/settingsV2/platform/slack/utils';
import { useAppConfig } from '@app/useAppConfig';
import { PageTitle, Switch, TextArea, colors } from '@src/alchemy-components';

import { useConnectionQuery } from '@graphql/connection.generated';
import {
    useUpdateGlobalAiAssistantSettingsMutation,
    useUpdateGlobalDocsAiSettingsMutation,
    useUpdateGlobalIntegrationSettingsMutation,
} from '@graphql/settings.generated';
import { AiInstructionType } from '@types';

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
    &:hover {
        color: ${colors.violet['800']};
    }
`;

const StyledCard = styled.div`
    border: 1px solid ${colors.gray[100]};
    border-radius: 12px;
    box-shadow: 0px 1px 2px 0px rgba(33, 23, 95, 0.07);
    padding: 16px;
    margin-bottom: 16px;
`;

const CardHeader = styled.div`
    display: flex;
    justify-content: space-between;
    align-items: center;
`;

const InstructionsLabel = styled.div`
    font-family: Mulish, sans-serif;
    font-size: 12px;
    font-weight: 700;
    color: ${colors.gray[1700]};
    margin-top: 16px;
    margin-bottom: 4px;
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

const InstructionsContainer = styled.div`
    margin-top: 0;
`;

const CharacterCount = styled.div`
    font-size: 12px;
    color: ${colors.gray[500]};
    text-align: right;
    margin-top: 4px;
`;

const SlackToggleSection = styled.div`
    margin-top: 16px;
    padding-top: 16px;
    border-top: 1px solid ${colors.gray[100]};
`;

export const PlatformAiSettings = () => {
    const appConfig = useAppConfig();
    const aiPluginsEnabled = appConfig.config?.featureFlags?.aiPluginsEnabled ?? false;
    const { globalSettings, refetch, loading } = useGlobalSettingsContext();
    const [updateGlobalDocsAiSettings, { loading: updatingDocsAi }] = useUpdateGlobalDocsAiSettingsMutation();
    const [updateGlobalAiAssistantSettings, { loading: updatingAiAssistant }] =
        useUpdateGlobalAiAssistantSettingsMutation();
    const [updateGlobalIntegrationSettings, { loading: updatingSlack }] = useUpdateGlobalIntegrationSettingsMutation();

    // Documentation AI settings
    const aiEnabled = globalSettings?.documentationAi?.enabled ?? false;
    const docAiInstructions = globalSettings?.documentationAi?.instructions?.[0]?.instruction ?? '';
    const [docAiSwitchValue, setDocAiSwitchValue] = useState(aiEnabled);
    const [docAiInstructionsValue, setDocAiInstructionsValue] = useState(docAiInstructions);

    // AI Assistant settings
    const aiAssistantInstructions = globalSettings?.aiAssistant?.instructions?.[0]?.instruction ?? '';
    const [aiAssistantInstructionsValue, setAiAssistantInstructionsValue] = useState(aiAssistantInstructions);

    // Slack bot settings
    const slackBotEnabled = globalSettings?.integrationSettings?.slackSettings?.datahubAtMentionEnabled ?? false;
    const [slackBotSwitchValue, setSlackBotSwitchValue] = useState(slackBotEnabled);

    // Get Slack connection to check if it's configured
    const { data: connData } = useConnectionQuery({
        variables: {
            urn: SLACK_CONNECTION_URN,
        },
    });

    const existingConnJson = connData?.connection?.details?.json;
    const slackConnData = existingConnJson && decodeSlackConnection(existingConnJson.blob as string);
    const isSlackEnabled = !!slackConnData?.botToken;

    React.useEffect(() => {
        setDocAiSwitchValue(aiEnabled);
        setDocAiInstructionsValue(docAiInstructions);
        setAiAssistantInstructionsValue(aiAssistantInstructions);
        setSlackBotSwitchValue(slackBotEnabled);
    }, [aiEnabled, docAiInstructions, aiAssistantInstructions, slackBotEnabled]);

    const handleDocsAiToggle = (checked: boolean) => {
        const instructions = docAiInstructionsValue.trim()
            ? [{ type: AiInstructionType.GeneralContext, instruction: docAiInstructionsValue.trim() }]
            : [];

        updateGlobalDocsAiSettings({
            variables: {
                input: {
                    enabled: checked,
                    instructions,
                },
            },
        })
            .then((res) => {
                if (res.data?.updateGlobalSettings) {
                    message.success(`AI documentation generation ${checked ? 'enabled' : 'disabled'}`);
                    return refetch();
                }
                message.error('Failed to update AI documentation setting');
                return Promise.resolve();
            })
            .catch(() => {
                message.error('Failed to update AI documentation setting');
            });
    };

    const handleDocsAiInstructionsUpdate = () => {
        const instructions = docAiInstructionsValue.trim()
            ? [{ type: AiInstructionType.GeneralContext, instruction: docAiInstructionsValue.trim() }]
            : [];

        updateGlobalDocsAiSettings({
            variables: {
                input: {
                    enabled: docAiSwitchValue,
                    instructions,
                },
            },
        })
            .then((res) => {
                if (res.data?.updateGlobalSettings) {
                    message.success('Saved instructions!');
                    return refetch();
                }
                message.error('Failed to update documentation AI instructions');
                return Promise.resolve();
            })
            .catch(() => {
                message.error('Failed to update documentation AI instructions');
            });
    };

    const handleAiAssistantInstructionsUpdate = () => {
        const instructions = aiAssistantInstructionsValue.trim()
            ? [{ type: AiInstructionType.GeneralContext, instruction: aiAssistantInstructionsValue.trim() }]
            : [];

        updateGlobalAiAssistantSettings({
            variables: {
                input: { instructions },
            },
        })
            .then((res) => {
                if (res.data?.updateGlobalSettings) {
                    message.success('Saved instructions!');
                    return refetch();
                }
                message.error('Failed to update AI assistant instructions');
                return Promise.resolve();
            })
            .catch(() => {
                message.error('Failed to update AI assistant instructions');
            });
    };

    const handleSlackBotToggle = (checked: boolean) => {
        updateGlobalIntegrationSettings({
            variables: {
                input: {
                    slackSettings: {
                        datahubAtMentionEnabled: checked,
                    },
                },
            },
        })
            .then((res) => {
                if (res.data?.updateGlobalSettings) {
                    message.success(`Slack @DataHub bot ${checked ? 'enabled' : 'disabled'}`);
                    return refetch();
                }
                message.error('Failed to update Slack bot setting');
                return Promise.resolve();
            })
            .catch(() => {
                message.error('Failed to update Slack bot setting');
            });
    };

    let content: React.ReactNode = null;
    if (loading) {
        content = <Spin style={{ marginTop: 40 }} />;
    } else {
        content = (
            <>
                {/* Ask DataHub Section */}
                <StyledCard>
                    <SettingText data-testid="ai-assistant-section-title">Ask DataHub</SettingText>
                    <DescriptionText>
                        Customize how Ask DataHub responds to questions. These instructions apply to all conversations,
                        including across DataHub, Slack, & Microsoft Teams.
                    </DescriptionText>
                    <InstructionsLabel data-testid="ai-assistant-instructions-section">
                        Chat Instructions
                    </InstructionsLabel>
                    <InstructionsContainer>
                        <TextArea
                            label=""
                            placeholder="Add custom instructions to guide how Ask DataHub answers questions. Hint: provide specific details about concepts, processes, and anything else unique to your organization. Example: 'Our data warehouse uses Snowflake. The finance team owns all tables in the reporting schema.'"
                            value={aiAssistantInstructionsValue}
                            onChange={(e) => setAiAssistantInstructionsValue(e.target.value)}
                            onBlur={handleAiAssistantInstructionsUpdate}
                            maxLength={10000}
                            rows={5}
                            isDisabled={updatingAiAssistant}
                            data-testid="ai-assistant-instructions-textarea"
                        />
                        {aiAssistantInstructionsValue.length >= 8000 && (
                            <CharacterCount data-testid="ai-assistant-character-count">
                                {aiAssistantInstructionsValue.length} / 10,000 characters
                            </CharacterCount>
                        )}
                    </InstructionsContainer>

                    {/* Enable Ask DataHub in Slack - nested inside Ask DataHub card */}
                    <SlackToggleSection>
                        <CardHeader>
                            <TextContainer>
                                <SettingText>Enable Ask DataHub in Slack</SettingText>
                                <DescriptionText>
                                    When enabled, users can mention @DataHub in Slack to ask questions about your
                                    metadata. The{' '}
                                    <Link to="/settings/integrations/slack" style={{ color: colors.violet['600'] }}>
                                        Slack integration
                                    </Link>{' '}
                                    must be configured first.{' '}
                                    <DocsLink
                                        href="https://docs.datahub.com/docs/managed-datahub/slack/saas-slack-app"
                                        target="_blank"
                                        rel="noopener noreferrer"
                                    >
                                        Learn more about DataHub in Slack.
                                    </DocsLink>
                                </DescriptionText>
                            </TextContainer>
                            <Switch
                                label=""
                                isChecked={slackBotSwitchValue}
                                isDisabled={!isSlackEnabled || updatingSlack}
                                onChange={(e) => handleSlackBotToggle(e.target.checked)}
                                data-testid="slack-bot-toggle"
                                disabledHoverText={
                                    !isSlackEnabled
                                        ? 'Configure Slack integration first to enable @DataHub AI'
                                        : undefined
                                }
                            />
                        </CardHeader>
                    </SlackToggleSection>
                </StyledCard>

                {/* Enable AI Documentation Card */}
                <StyledCard>
                    <CardHeader>
                        <TextContainer>
                            <SettingText data-testid="ai-documentation-section-title">
                                AI Documentation Generation
                            </SettingText>
                            <DescriptionText>
                                When enabled, DataHub can generate documentation using AI for supported entities.{' '}
                                <DocsLink
                                    href="https://docs.datahub.com/docs/automations/ai-docs"
                                    target="_blank"
                                    rel="noopener noreferrer"
                                >
                                    Learn more about AI-powered documentation.
                                </DocsLink>
                            </DescriptionText>
                        </TextContainer>
                        <Switch
                            label=""
                            isChecked={docAiSwitchValue}
                            isDisabled={updatingDocsAi}
                            onChange={(e) => handleDocsAiToggle(e.target.checked)}
                            data-testid="ai-docs-toggle"
                        />
                    </CardHeader>
                    {docAiSwitchValue && (
                        <InstructionsContainer>
                            <InstructionsLabel data-testid="docs-ai-instructions-section">
                                Documentation Instructions
                            </InstructionsLabel>
                            <TextArea
                                label=""
                                placeholder="Add custom instructions to guide how AI generates table and column descriptions. Hint: provide specific details about concepts, processes, and anything else unique to your organization. Example: 'Always mention the data source and refresh frequency. Use business-friendly language.'"
                                value={docAiInstructionsValue}
                                onChange={(e) => setDocAiInstructionsValue(e.target.value)}
                                onBlur={handleDocsAiInstructionsUpdate}
                                maxLength={10000}
                                rows={5}
                                isDisabled={updatingDocsAi}
                                data-testid="docs-ai-instructions-textarea"
                            />
                            {docAiInstructionsValue.length >= 8000 && (
                                <CharacterCount data-testid="docs-ai-character-count">
                                    {docAiInstructionsValue.length} / 10,000 characters
                                </CharacterCount>
                            )}
                        </InstructionsContainer>
                    )}
                </StyledCard>

                {/* AI Plugins Section - behind feature flag */}
                {aiPluginsEnabled && <AiPluginsSettings />}
            </>
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
