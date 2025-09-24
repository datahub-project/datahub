import { InfoCircleFilled } from '@ant-design/icons';
import { Spin, message } from 'antd';
import React, { useState } from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

import { useGlobalSettingsContext } from '@app/context/GlobalSettings/GlobalSettingsContext';
import { SLACK_CONNECTION_URN } from '@app/settingsV2/platform/slack/constants';
import { decodeSlackConnection } from '@app/settingsV2/platform/slack/utils';
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

const SectionTitle = styled.h2`
    font-size: 20px;
    font-weight: 600;
    color: ${colors.gray[600]};
    margin: 32px 0 16px 0;
    &:first-of-type {
        margin-top: 24px;
    }
`;

const SubSection = styled.div`
    margin-bottom: 32px;
`;

const SubSectionTitle = styled.h3`
    font-size: 16px;
    font-weight: 600;
    color: ${colors.gray[1700]};
    margin: 24px 0 8px 2px;
`;

const InstructionsContainer = styled.div`
    margin-top: 0px;
`;

const CharacterCount = styled.div`
    font-size: 12px;
    color: ${colors.gray[500]};
    text-align: right;
    margin-top: 4px;
`;

export const PlatformAiSettings = () => {
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
                {/* AI Documentation Section */}
                <SectionTitle data-testid="ai-documentation-section-title">AI Documentation</SectionTitle>
                <SubSection>
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
                            isChecked={docAiSwitchValue}
                            isDisabled={updatingDocsAi}
                            onChange={(e) => handleDocsAiToggle(e.target.checked)}
                            data-testid="ai-docs-toggle"
                        />
                    </StyledCard>
                </SubSection>
                <SubSection>
                    <SubSectionTitle data-testid="docs-ai-instructions-section">Instructions</SubSectionTitle>
                    <InstructionsContainer>
                        <TextArea
                            label=""
                            placeholder="Add custom instructions to help guide the model output. Hint: provide specific details about concepts, processes, and anything else unique to your organization."
                            value={docAiInstructionsValue}
                            onChange={(e) => setDocAiInstructionsValue(e.target.value)}
                            onBlur={handleDocsAiInstructionsUpdate}
                            maxLength={10000}
                            rows={6}
                            isDisabled={updatingDocsAi}
                            data-testid="docs-ai-instructions-textarea"
                        />
                        <CharacterCount data-testid="docs-ai-character-count">
                            {docAiInstructionsValue.length} / 10,000 characters
                        </CharacterCount>
                    </InstructionsContainer>
                </SubSection>

                {/* Ask DataHub (AI Assistant) Section */}
                <SectionTitle data-testid="ai-assistant-section-title">Ask DataHub (AI Assistant)</SectionTitle>
                <SubSection>
                    <StyledCard>
                        <TextContainer>
                            <SettingText>Enable Ask DataHub in Slack</SettingText>
                            <DescriptionText>
                                When enabled, users can mention @DataHub in Slack to ask questions about your metadata.
                                The{' '}
                                <Link to="/settings/integrations/slack" style={{ color: colors.violet['600'] }}>
                                    Slack integration
                                </Link>{' '}
                                must be configured first.
                            </DescriptionText>
                            <DocsLink
                                href="https://docs.datahub.com/docs/managed-datahub/slack/saas-slack-app"
                                target="_blank"
                                rel="noopener noreferrer"
                            >
                                Learn more about DataHub in Slack.
                            </DocsLink>
                        </TextContainer>
                        <Switch
                            label=""
                            isChecked={slackBotSwitchValue}
                            isDisabled={!isSlackEnabled || updatingSlack}
                            onChange={(e) => handleSlackBotToggle(e.target.checked)}
                            data-testid="slack-bot-toggle"
                            disabledHoverText={
                                !isSlackEnabled ? 'Configure Slack integration first to enable @DataHub AI' : undefined
                            }
                        />
                    </StyledCard>
                </SubSection>
                <SubSection>
                    <SubSectionTitle data-testid="ai-assistant-instructions-section">Instructions</SubSectionTitle>
                    <InstructionsContainer>
                        <TextArea
                            label=""
                            placeholder="Add custom instructions to help guide the model output. Hint: provide specific details about concepts, processes, and anything else unique to your organization."
                            value={aiAssistantInstructionsValue}
                            onChange={(e) => setAiAssistantInstructionsValue(e.target.value)}
                            onBlur={handleAiAssistantInstructionsUpdate}
                            maxLength={10000}
                            rows={6}
                            isDisabled={updatingAiAssistant}
                            data-testid="ai-assistant-instructions-textarea"
                        />
                        <CharacterCount data-testid="ai-assistant-character-count">
                            {aiAssistantInstructionsValue.length} / 10,000 characters
                        </CharacterCount>
                    </InstructionsContainer>
                </SubSection>
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
