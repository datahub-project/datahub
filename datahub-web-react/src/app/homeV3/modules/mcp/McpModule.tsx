import { Text } from '@components';
import { PlugsConnected } from '@phosphor-icons/react/dist/csr/PlugsConnected';
import React, { useCallback, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { useHistory } from 'react-router';
import styled from 'styled-components';

import EmptyContent from '@app/homeV3/module/components/EmptyContent';
import LargeModule from '@app/homeV3/module/components/LargeModule';
import { ModuleProps } from '@app/homeV3/module/types';
import McpClientSetupModal from '@app/homeV3/modules/mcp/McpClientSetupModal';
import { McpClientTile } from '@app/homeV3/modules/mcp/McpModule.components';
import { MCP_SETUP_DOCS_URL, McpModuleClient, getMcpModuleClients } from '@app/homeV3/modules/mcp/mcpModule.utils';
import { useIsMcpEnabled } from '@app/homeV3/modules/mcp/useIsMcpEnabled';
import { AiTabType, aiTabUrl } from '@app/settingsV2/platform/PlatformAiSettings.types';
import { useCanManageMcpServers } from '@app/settingsV2/platform/ai/mcpServers/hooks/useCanManageMcpServers';
import { buildConnectionUrl } from '@app/settingsV2/platform/ai/mcpServers/utils/mcpServerDataUtils';

const Content = styled.div`
    display: flex;
    flex-direction: column;
    gap: 12px;
    padding: 0 6px;
    color: ${(props) => props.theme.colors.text};
`;

const ClientList = styled.div`
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 8px;
`;

const MCP_SETTINGS_PATH = aiTabUrl(AiTabType.McpServers);

function openInNewTab(url: string) {
    window.open(url, '_blank', 'noopener,noreferrer');
}

/**
 * Home page module with setup buttons for connecting AI tools to DataHub's MCP server. Each
 * opens the same setup the settings page's Connect card shows, at that client's tab. Admins who
 * manage the MCP server get a link to its settings in the footer; everyone else gets the docs.
 *
 * While an admin has turned MCP off, the template hides the module (see useIsModuleHidden), so it
 * doesn't advertise a connection that would fail. The only time it renders then is while an admin
 * edits the org default, where it shows a placeholder they can remove.
 */
export default function McpModule(props: ModuleProps) {
    const { t } = useTranslation('modules');
    const history = useHistory();
    const canManageMcpServers = useCanManageMcpServers();
    const [openClient, setOpenClient] = useState<McpModuleClient | null>(null);
    const { isMcpEnabled, loading } = useIsMcpEnabled();
    const clients = getMcpModuleClients(buildConnectionUrl());

    const handleFooterClick = useCallback(() => {
        if (canManageMcpServers) history.push(MCP_SETTINGS_PATH);
        else openInNewTab(MCP_SETUP_DOCS_URL);
    }, [canManageMcpServers, history]);

    if (!isMcpEnabled) {
        return (
            <LargeModule {...props} dataTestId="mcp-module">
                <EmptyContent
                    icon={PlugsConnected}
                    title={t('mcp.disabledTitle')}
                    description={t('mcp.disabledDescription')}
                    linkText={canManageMcpServers ? t('mcp.openSettings') : undefined}
                    onLinkClick={() => history.push(MCP_SETTINGS_PATH)}
                    dataTestId="mcp-module-disabled"
                />
            </LargeModule>
        );
    }

    const clientName = (client: McpModuleClient) => client.name ?? t('mcp.otherClient');

    return (
        <>
            <LargeModule
                {...props}
                loading={loading}
                onClickViewAll={handleFooterClick}
                viewAllText={canManageMcpServers ? t('mcp.manageServer') : t('mcp.viewGuide')}
                dataTestId="mcp-module"
            >
                <Content>
                    <Text size="sm" color="gray">
                        {t('mcp.intro')}
                    </Text>
                    <ClientList>
                        {clients.map((client) => (
                            <McpClientTile
                                key={client.key}
                                clientKey={client.key}
                                name={clientName(client)}
                                onClick={() => setOpenClient(client)}
                            />
                        ))}
                    </ClientList>
                </Content>
            </LargeModule>
            {openClient && (
                <McpClientSetupModal
                    name={openClient.name}
                    client={openClient.client}
                    onClose={() => setOpenClient(null)}
                />
            )}
        </>
    );
}
