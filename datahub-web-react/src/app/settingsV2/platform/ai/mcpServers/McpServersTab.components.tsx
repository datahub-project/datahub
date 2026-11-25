import { Copy } from '@phosphor-icons/react/dist/csr/Copy';
import { PencilSimple } from '@phosphor-icons/react/dist/csr/PencilSimple';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import type { McpServerRow } from '@app/settingsV2/platform/ai/mcpServers/utils/mcpServerDataUtils';
import { Button, Switch, Text } from '@src/alchemy-components';

const Card = styled.section`
    display: flex;
    flex-direction: column;
    gap: 16px;
    flex-shrink: 0;
    padding: 16px 20px;
    border: 1px solid ${(props) => props.theme.colors.border};
    border-radius: 12px;
    /* Set the semantic text color here so the alchemy <Text> children (color="inherit")
       pick it up instead of falling back to the browser default. */
    color: ${(props) => props.theme.colors.text};
`;

const HeaderRow = styled.div`
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 16px;
`;

const HeaderText = styled.div`
    display: flex;
    flex-direction: column;
    gap: 2px;
`;

const Details = styled.dl`
    display: grid;
    grid-template-columns: max-content minmax(0, 1fr);
    align-items: center;
    column-gap: 24px;
    row-gap: 12px;
    margin: 0;
    padding-top: 16px;
    border-top: 1px solid ${(props) => props.theme.colors.border};
`;

const Label = styled.dt`
    margin: 0;
`;

const Value = styled.dd`
    display: flex;
    align-items: center;
    gap: 6px;
    min-width: 0;
    margin: 0;
`;

const UrlText = styled.span`
    min-width: 0;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    font-family: monospace;
    font-size: 12px;
`;

const Actions = styled.div`
    display: flex;
    justify-content: flex-end;
`;

type McpSingleServerCardProps = {
    server: McpServerRow;
    /** Whether /mcp is served: the master switch and the server's own enabled flag both on. */
    enabled: boolean;
    /** The settings are still loading, so `enabled` is a guess: the switch waits. */
    loading: boolean;
    onToggle: (enabled: boolean) => void;
    onEdit: () => void;
    onCopyUrl: (url: string) => void;
    /** The tools the server exposes, as pills. */
    tools: React.ReactNode;
};

/**
 * DataHub's one MCP server, shown when scoped servers are off: a single on/off switch and, while
 * on, the server's connection URL, its tools, and an edit button.
 */
export function McpSingleServerCard({
    server,
    enabled,
    loading,
    onToggle,
    onEdit,
    onCopyUrl,
    tools,
}: McpSingleServerCardProps) {
    const { t } = useTranslation('settings.ai');
    const { t: tc } = useTranslation('common.actions');

    return (
        <Card data-testid="mcp-server-card">
            <HeaderRow>
                <HeaderText>
                    <Text weight="bold" size="lg">
                        {t('platform.ai.mcpServer.switchLabel')}
                    </Text>
                    <Text color="gray">{t('platform.ai.mcpServer.switchHelp')}</Text>
                </HeaderText>
                <Switch
                    label=""
                    isChecked={enabled}
                    isDisabled={loading}
                    onChange={(e) => onToggle(e.target.checked)}
                    data-testid="mcp-master-switch"
                />
            </HeaderRow>
            {enabled && !loading && (
                <>
                    <Details>
                        <Label>
                            <Text color="gray">{t('platform.ai.mcpServers.columnConnectionUrl')}</Text>
                        </Label>
                        <Value>
                            <UrlText title={server.connectionUrl} data-testid="mcp-server-connection-url">
                                {server.connectionUrl}
                            </UrlText>
                            <Button
                                variant="text"
                                size="sm"
                                onClick={() => onCopyUrl(server.connectionUrl)}
                                icon={{ icon: Copy }}
                                aria-label={t('platform.ai.mcpServers.copyUrl')}
                            />
                        </Value>
                        <Label>
                            <Text color="gray">{t('platform.ai.mcpServers.columnTools')}</Text>
                        </Label>
                        <Value>{tools}</Value>
                    </Details>
                    <Actions>
                        <Button
                            variant="outline"
                            size="sm"
                            icon={{ icon: PencilSimple }}
                            onClick={onEdit}
                            data-testid={`mcp-server-edit-${server.id}`}
                        >
                            {tc('edit')}
                        </Button>
                    </Actions>
                </>
            )}
        </Card>
    );
}
