import { Copy } from '@phosphor-icons/react/dist/csr/Copy';
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

import { McpClient, buildMcpClientSetups } from '@app/settingsV2/platform/ai/mcpServers/McpConnectCard.utils';
import { buildConnectionUrl } from '@app/settingsV2/platform/ai/mcpServers/utils/mcpServerDataUtils';
import { useAppConfig } from '@app/useAppConfig';
import { Alert, Button, TabButtons, Text, toast } from '@src/alchemy-components';

const MCP_DOCS_URL = 'https://docs.datahub.com/docs/features/feature-guides/mcp';
const ACCESS_TOKENS_PATH = '/settings/tokens';

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

const Header = styled.div`
    display: flex;
    flex-direction: column;
    gap: 2px;
`;

const AuthRow = styled.div`
    display: flex;
    align-items: center;
    justify-content: space-between;
    flex-wrap: wrap;
    gap: 12px;
`;

const AuthText = styled.div`
    display: flex;
    flex-direction: column;
    gap: 2px;
    flex: 1;
    min-width: 240px;
`;

const SnippetHeader = styled.div`
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 8px;
`;

const Snippet = styled.pre`
    margin: 0;
    padding: 12px;
    border: 1px solid ${(props) => props.theme.colors.border};
    border-radius: 8px;
    background: ${(props) => props.theme.colors.bgSurface};
    color: ${(props) => props.theme.colors.text};
    font-family: monospace;
    font-size: 12px;
    white-space: pre-wrap;
    word-break: break-all;
`;

const DocsLink = styled.a`
    color: ${(props) => props.theme.colors.hyperlinks};
`;

const TokenLink = styled(Link)`
    font-size: 14px;
    font-weight: 600;
    color: ${(props) => props.theme.colors.hyperlinks};
`;

/**
 * How to connect an MCP client to DataHub's default MCP server: its URL, how requests
 * authenticate (following DataHub's token auth setting), and a ready-to-paste setup
 * for each common client.
 */
export function McpConnectCard() {
    const { t } = useTranslation('settings.ai');
    const { config } = useAppConfig();
    const tokenAuth = !!config.authConfig?.tokenAuthEnabled;
    const setups = buildMcpClientSetups(buildConnectionUrl(), tokenAuth);
    const [activeClient, setActiveClient] = useState<string>(McpClient.ClaudeCode);
    const active = setups.find((setup) => setup.client === activeClient) ?? setups[0];

    const handleCopy = () => {
        navigator.clipboard?.writeText(active.snippet);
        toast.success(t('platform.ai.mcpConnect.copied'));
    };

    return (
        <Card aria-label={t('platform.ai.mcpConnect.title')} data-testid="mcp-connect-card">
            <Header>
                <Text weight="bold" size="lg">
                    {t('platform.ai.mcpConnect.title')}
                </Text>
                <Text color="gray">{t('platform.ai.mcpConnect.subtitle')}</Text>
            </Header>

            {tokenAuth ? (
                <AuthRow data-testid="mcp-connect-token-auth">
                    <AuthText>
                        <Text weight="bold">{t('platform.ai.mcpConnect.tokenAuthTitle')}</Text>
                        <Text color="gray">{t('platform.ai.mcpConnect.tokenAuthBody')}</Text>
                    </AuthText>
                    {/* The app router's basename adds the base path DataHub is served under. */}
                    <TokenLink to={ACCESS_TOKENS_PATH} data-testid="mcp-connect-token-link">
                        {t('platform.ai.mcpConnect.createToken')}
                    </TokenLink>
                </AuthRow>
            ) : (
                <Alert
                    variant="warning"
                    title={t('platform.ai.mcpConnect.noAuthTitle')}
                    description={t('platform.ai.mcpConnect.noAuthBody')}
                    data-testid="mcp-connect-no-auth"
                />
            )}

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

            <div>
                <SnippetHeader>
                    <Text color="gray" size="sm">
                        {active.configFile
                            ? t('platform.ai.mcpConnect.addToFile', { file: active.configFile })
                            : t(
                                  active.client === McpClient.ClaudeCode
                                      ? 'platform.ai.mcpConnect.runInTerminal'
                                      : 'platform.ai.mcpConnect.otherHint',
                              )}
                    </Text>
                    <Button variant="text" size="sm" icon={{ icon: Copy }} onClick={handleCopy}>
                        {t('platform.ai.mcpConnect.copy')}
                    </Button>
                </SnippetHeader>
                <Snippet data-testid="mcp-connect-snippet">{active.snippet}</Snippet>
            </div>

            {active.client === McpClient.ClaudeDesktop && (
                <Text color="gray" size="sm">
                    {t('platform.ai.mcpConnect.claudeDesktopNote')}
                </Text>
            )}

            <Text color="gray" size="sm">
                {t('platform.ai.mcpConnect.docsPrompt')}{' '}
                <DocsLink href={MCP_DOCS_URL} target="_blank" rel="noopener noreferrer">
                    {t('platform.ai.mcpConnect.docsLink')}
                </DocsLink>
            </Text>
        </Card>
    );
}
