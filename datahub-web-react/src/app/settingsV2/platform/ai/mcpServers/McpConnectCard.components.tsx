import { Copy } from '@phosphor-icons/react/dist/csr/Copy';
import React from 'react';
import { Trans, useTranslation } from 'react-i18next';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

import {
    type McpChatGptSetup,
    McpClient,
    type McpConnectorSetup,
    type McpSnippetSetup,
    isPublicHttpsUrl,
    preferredClaudeClient,
} from '@app/settingsV2/platform/ai/mcpServers/McpConnectCard.utils';
import { Alert, Button, Text, toast } from '@src/alchemy-components';

const ACCESS_TOKENS_PATH = '/settings/tokens';
const CHATGPT_PLUGINS_URL = 'https://chatgpt.com/plugins';
const SECURE_MCP_TUNNEL_DOCS_URL = 'https://developers.openai.com/api/docs/guides/secure-mcp-tunnels';

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

const TokenLink = styled(Link)`
    font-size: 14px;
    font-weight: 600;
    color: ${(props) => props.theme.colors.hyperlinks};
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

const SetupColumn = styled.div`
    display: flex;
    flex-direction: column;
    gap: 12px;
`;

const Steps = styled.ol`
    margin: 0;
    padding-left: 20px;
    display: flex;
    flex-direction: column;
    gap: 8px;
    color: ${(props) => props.theme.colors.text};
`;

const Fields = styled.div`
    display: flex;
    flex-direction: column;
    gap: 4px;
    margin-top: 6px;
`;

const FieldRow = styled.div`
    display: grid;
    grid-template-columns: minmax(80px, max-content) minmax(0, 1fr) auto;
    align-items: center;
    gap: 8px;
`;

const FieldValue = styled.code`
    min-width: 0;
    padding: 4px 8px;
    border: 1px solid ${(props) => props.theme.colors.border};
    border-radius: 6px;
    background: ${(props) => props.theme.colors.bgSurface};
    color: ${(props) => props.theme.colors.text};
    font-family: monospace;
    font-size: 12px;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
`;

// Inline text button that switches the setup to another client.
const SwitchClientButton = styled.button`
    padding: 0;
    border: none;
    background: none;
    cursor: pointer;
    font: inherit;
    font-weight: 600;
    color: ${(props) => props.theme.colors.hyperlinks};
    &:hover {
        text-decoration: underline;
    }
`;

const ExternalLink = styled.a`
    font-weight: 600;
    color: ${(props) => props.theme.colors.hyperlinks};
`;

function useCopy() {
    const { t } = useTranslation('settings.ai');
    return (value: string) => {
        navigator.clipboard?.writeText(value);
        toast.success(t('platform.ai.mcpConnect.copied'));
    };
}

/** How requests to DataHub's MCP server authenticate, following DataHub's token auth setting. */
export function McpAuthNotice({ tokenAuth }: { tokenAuth: boolean }) {
    const { t } = useTranslation('settings.ai');
    if (!tokenAuth) {
        return (
            <Alert
                variant="warning"
                title={t('platform.ai.mcpConnect.noAuthTitle')}
                description={t('platform.ai.mcpConnect.noAuthBody')}
                data-testid="mcp-connect-no-auth"
            />
        );
    }
    return (
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
    );
}

/** A config snippet or command to paste, with a copy button. */
export function McpSnippetSetupView({ setup }: { setup: McpSnippetSetup }) {
    const { t } = useTranslation('settings.ai');
    const copy = useCopy();
    let hint = t('platform.ai.mcpConnect.otherHint');
    if (setup.configFile) hint = t('platform.ai.mcpConnect.addToFile', { file: setup.configFile });
    else if (setup.client === McpClient.ClaudeCode) hint = t('platform.ai.mcpConnect.runInTerminal');

    return (
        <SetupColumn>
            <div>
                <SnippetHeader>
                    <Text color="gray" size="sm">
                        {hint}
                    </Text>
                    <Button variant="text" size="sm" icon={{ icon: Copy }} onClick={() => copy(setup.snippet)}>
                        {t('platform.ai.mcpConnect.copy')}
                    </Button>
                </SnippetHeader>
                <Snippet data-testid="mcp-connect-snippet">{setup.snippet}</Snippet>
            </div>
            {setup.client === McpClient.ClaudeDesktop && (
                <Text color="gray" size="sm">
                    {t('platform.ai.mcpConnect.claudeDesktopNote')}
                </Text>
            )}
        </SetupColumn>
    );
}

function CopyField({ label, value, testId }: { label: string; value: string; testId: string }) {
    const { t } = useTranslation('settings.ai');
    const copy = useCopy();
    return (
        <FieldRow>
            <Text color="gray" size="sm">
                {label}
            </Text>
            <FieldValue title={value} data-testid={testId}>
                {value}
            </FieldValue>
            <Button
                variant="text"
                size="sm"
                icon={{ icon: Copy }}
                onClick={() => copy(value)}
                aria-label={t('platform.ai.mcpConnect.copyField', { field: label })}
            />
        </FieldRow>
    );
}

type ConnectorSetupProps = {
    setup: McpConnectorSetup;
    /** Switches the setup to another client, for the "use the local bridge instead" links. */
    onSelectClient: (client: McpClient) => void;
};

/**
 * Steps for adding DataHub as a Claude custom connector (claude.ai and Claude Desktop), which
 * Claude calls from Anthropic's cloud. Field names follow Anthropic's "Add custom connector"
 * dialog: https://claude.com/docs/connectors/custom/add-unlisted
 */
export function McpConnectorSetupView({ setup, onSelectClient }: ConnectorSetupProps) {
    const { t } = useTranslation('settings.ai');
    const isPublic = isPublicHttpsUrl(setup.url);
    const strong = <strong />;
    const switchToLocal = (
        <SwitchClientButton
            type="button"
            onClick={() => onSelectClient(McpClient.ClaudeDesktop)}
            data-testid="mcp-connect-use-local"
        />
    );

    return (
        <SetupColumn data-testid="mcp-connect-connector">
            {isPublic ? (
                <Text color="gray" size="sm" data-testid="mcp-connect-connector-recommended">
                    {t('platform.ai.mcpConnect.connectorRecommended')}
                </Text>
            ) : (
                <Alert
                    variant="warning"
                    title={t('platform.ai.mcpConnect.connectorPrivateTitle')}
                    description={
                        <Trans
                            t={t}
                            i18nKey="platform.ai.mcpConnect.connectorPrivateBody"
                            components={{ local: switchToLocal }}
                        />
                    }
                    data-testid="mcp-connect-connector-private"
                />
            )}
            <Steps>
                <li>
                    <Text>
                        <Trans t={t} i18nKey="platform.ai.mcpConnect.connectorStepOpen" components={{ strong }} />
                    </Text>
                </li>
                <li>
                    <Text>{t('platform.ai.mcpConnect.connectorStepFields')}</Text>
                    <Fields>
                        <CopyField
                            label={t('platform.ai.mcpConnect.connectorNameField')}
                            value={setup.name}
                            testId="mcp-connect-connector-name"
                        />
                        <CopyField
                            label={t('platform.ai.mcpConnect.connectorUrlField')}
                            value={setup.url}
                            testId="mcp-connect-connector-url"
                        />
                    </Fields>
                </li>
                {setup.header ? (
                    <li>
                        <Text>
                            <Trans
                                t={t}
                                i18nKey="platform.ai.mcpConnect.connectorStepHeader"
                                values={{ header: setup.header.name }}
                                components={{ strong }}
                            />
                        </Text>
                        <Fields>
                            <CopyField
                                label={setup.header.name}
                                value={setup.header.value}
                                testId="mcp-connect-connector-header"
                            />
                        </Fields>
                    </li>
                ) : (
                    <li>
                        <Text>
                            <Trans t={t} i18nKey="platform.ai.mcpConnect.connectorStepNoAuth" components={{ strong }} />
                        </Text>
                    </li>
                )}
                <li>
                    <Text>
                        <Trans t={t} i18nKey="platform.ai.mcpConnect.connectorStepAdd" components={{ strong }} />
                    </Text>
                </li>
            </Steps>
            <Text color="gray" size="sm">
                <Trans
                    t={t}
                    i18nKey={
                        setup.header
                            ? 'platform.ai.mcpConnect.connectorTeamNoteToken'
                            : 'platform.ai.mcpConnect.connectorTeamNote'
                    }
                    components={{ strong }}
                />
            </Text>
            {setup.header && (
                <Text color="gray" size="sm" data-testid="mcp-connect-connector-headers-beta">
                    <Trans
                        t={t}
                        i18nKey="platform.ai.mcpConnect.connectorHeadersBeta"
                        components={{ strong, local: switchToLocal }}
                    />
                </Text>
            )}
        </SetupColumn>
    );
}

type ChatGptSetupProps = {
    setup: McpChatGptSetup;
    /** Switches the setup to another client, for the "use another client instead" links. */
    onSelectClient: (client: McpClient) => void;
};

/**
 * Steps for adding DataHub to ChatGPT as a custom MCP server, following OpenAI's guide:
 * https://developers.openai.com/api/docs/guides/custom-mcp-server. ChatGPT supports only OAuth
 * or no authentication there, and calls the server from OpenAI's servers, so it can connect only
 * to a public HTTPS URL with token auth off.
 */
export function McpChatGptSetupView({ setup, onSelectClient }: ChatGptSetupProps) {
    const { t } = useTranslation('settings.ai');
    const strong = <strong />;
    const switchTo = (client: McpClient, testId: string) => (
        <SwitchClientButton type="button" onClick={() => onSelectClient(client)} data-testid={testId} />
    );
    const otherClients = {
        claude: switchTo(preferredClaudeClient(setup.url), 'mcp-connect-chatgpt-use-claude'),
        cursor: switchTo(McpClient.Cursor, 'mcp-connect-chatgpt-use-cursor'),
        claudeCode: switchTo(McpClient.ClaudeCode, 'mcp-connect-chatgpt-use-claude-code'),
    };

    if (setup.tokenAuth) {
        return (
            <Alert
                variant="info"
                title={t('platform.ai.mcpConnect.chatGptTokenAuthTitle')}
                description={
                    <Trans t={t} i18nKey="platform.ai.mcpConnect.chatGptTokenAuthBody" components={otherClients} />
                }
                data-testid="mcp-connect-chatgpt-token-auth"
            />
        );
    }

    if (!isPublicHttpsUrl(setup.url)) {
        return (
            <Alert
                variant="warning"
                title={t('platform.ai.mcpConnect.chatGptPrivateTitle')}
                description={
                    <Trans
                        t={t}
                        i18nKey="platform.ai.mcpConnect.chatGptPrivateBody"
                        components={{
                            ...otherClients,
                            // Trans fills the anchor with the translated link text.
                            // eslint-disable-next-line jsx-a11y/anchor-has-content
                            tunnel: (
                                <ExternalLink
                                    href={SECURE_MCP_TUNNEL_DOCS_URL}
                                    target="_blank"
                                    rel="noopener noreferrer"
                                />
                            ),
                        }}
                    />
                }
                data-testid="mcp-connect-chatgpt-private"
            />
        );
    }

    return (
        <SetupColumn data-testid="mcp-connect-chatgpt">
            <Alert
                variant="error"
                title={t('platform.ai.mcpConnect.chatGptPublicNoAuthTitle')}
                description={t('platform.ai.mcpConnect.chatGptPublicNoAuthBody')}
                data-testid="mcp-connect-chatgpt-public-no-auth"
            />
            <Steps>
                <li>
                    <Text>
                        <Trans
                            t={t}
                            i18nKey="platform.ai.mcpConnect.chatGptStepOpen"
                            components={{
                                strong,
                                // eslint-disable-next-line jsx-a11y/anchor-has-content
                                plugins: (
                                    <ExternalLink
                                        href={CHATGPT_PLUGINS_URL}
                                        target="_blank"
                                        rel="noopener noreferrer"
                                    />
                                ),
                            }}
                        />
                    </Text>
                </li>
                <li>
                    <Text>
                        <Trans t={t} i18nKey="platform.ai.mcpConnect.chatGptStepFields" components={{ strong }} />
                    </Text>
                    <Fields>
                        <CopyField
                            label={t('platform.ai.mcpConnect.connectorNameField')}
                            value={setup.name}
                            testId="mcp-connect-chatgpt-name"
                        />
                        <CopyField
                            label={t('platform.ai.mcpConnect.chatGptUrlField')}
                            value={setup.url}
                            testId="mcp-connect-chatgpt-url"
                        />
                    </Fields>
                </li>
                <li>
                    <Text>
                        <Trans t={t} i18nKey="platform.ai.mcpConnect.chatGptStepNoAuth" components={{ strong }} />
                    </Text>
                </li>
                <li>
                    <Text>
                        <Trans t={t} i18nKey="platform.ai.mcpConnect.chatGptStepCreate" components={{ strong }} />
                    </Text>
                </li>
                <li>
                    <Text>
                        <Trans t={t} i18nKey="platform.ai.mcpConnect.chatGptStepUse" components={{ strong }} />
                    </Text>
                </li>
            </Steps>
            <Text color="gray" size="sm">
                {t('platform.ai.mcpConnect.chatGptWorkspaceNote')}
            </Text>
        </SetupColumn>
    );
}
