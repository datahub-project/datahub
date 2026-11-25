import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import {
    McpAuthNotice,
    McpChatGptSetupView,
    McpConnectorSetupView,
    McpSnippetSetupView,
} from '@app/settingsV2/platform/ai/mcpServers/McpConnectCard.components';
import { McpClient, buildMcpClientSetups } from '@app/settingsV2/platform/ai/mcpServers/McpConnectCard.utils';
import { buildConnectionUrl } from '@app/settingsV2/platform/ai/mcpServers/utils/mcpServerDataUtils';
import { useAppConfig } from '@app/useAppConfig';
import { TabButtons, Text } from '@src/alchemy-components';

export const MCP_DOCS_URL = 'https://docs.datahub.com/docs/features/feature-guides/mcp';

const Card = styled.section`
    display: flex;
    flex-direction: column;
    gap: 16px;
    flex-shrink: 0;
    padding: 16px 20px;
    border: 1px solid ${(props) => props.theme.colors.border};
    border-radius: 12px;
    /* Set the text color here so the alchemy <Text> children (color="inherit") pick it up. */
    color: ${(props) => props.theme.colors.text};
`;

const SetupContainer = styled.div`
    display: flex;
    flex-direction: column;
    gap: 16px;
    color: ${(props) => props.theme.colors.text};
`;

const Header = styled.div`
    display: flex;
    flex-direction: column;
    gap: 2px;
`;

const DocsLink = styled.a`
    color: ${(props) => props.theme.colors.hyperlinks};
`;

type McpConnectSetupProps = {
    /** The client whose setup shows first. */
    initialClient?: McpClient;
};

/**
 * How to connect an MCP client to DataHub's default MCP server: how requests authenticate
 * (following DataHub's token auth setting), and the setup for each common client. Shared by
 * the settings page and the home page MCP module.
 */
export function McpConnectSetup({ initialClient = McpClient.ClaudeCode }: McpConnectSetupProps) {
    const { t } = useTranslation('settings.ai');
    const { config } = useAppConfig();
    const tokenAuth = !!config.authConfig?.tokenAuthEnabled;
    const setups = buildMcpClientSetups(buildConnectionUrl(), tokenAuth);
    const [activeClient, setActiveClient] = useState<string>(initialClient);
    const active = setups.find((setup) => setup.client === activeClient) ?? setups[0];

    return (
        <SetupContainer>
            <McpAuthNotice tokenAuth={tokenAuth} />

            <TabButtons
                tabs={setups.map((setup) => ({
                    key: setup.client,
                    label: setup.client === McpClient.Other ? t('platform.ai.mcpConnect.otherClient') : setup.label,
                    dataTestId: `mcp-connect-client-${setup.client}`,
                }))}
                activeTab={active.client}
                onTabClick={setActiveClient}
                fit="hug"
                aria-label={t('platform.ai.mcpConnect.clientsLabel')}
            />

            {active.kind === 'connector' && <McpConnectorSetupView setup={active} onSelectClient={setActiveClient} />}
            {active.kind === 'chatgpt' && <McpChatGptSetupView setup={active} onSelectClient={setActiveClient} />}
            {active.kind === 'snippet' && <McpSnippetSetupView setup={active} />}

            <Text color="gray" size="sm">
                {t('platform.ai.mcpConnect.docsPrompt')}{' '}
                <DocsLink href={MCP_DOCS_URL} target="_blank" rel="noopener noreferrer">
                    {t('platform.ai.mcpConnect.docsLink')}
                </DocsLink>
            </Text>
        </SetupContainer>
    );
}

/** The settings page card around McpConnectSetup. */
export function McpConnectCard() {
    const { t } = useTranslation('settings.ai');

    return (
        <Card aria-label={t('platform.ai.mcpConnect.title')} data-testid="mcp-connect-card">
            <Header>
                <Text weight="bold" size="lg">
                    {t('platform.ai.mcpConnect.title')}
                </Text>
                <Text color="gray">{t('platform.ai.mcpConnect.subtitle')}</Text>
            </Header>
            <McpConnectSetup />
        </Card>
    );
}
