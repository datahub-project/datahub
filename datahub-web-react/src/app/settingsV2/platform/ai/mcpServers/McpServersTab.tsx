import { useApolloClient } from '@apollo/client';
import { Copy } from '@phosphor-icons/react/dist/csr/Copy';
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { Column } from '@components/components/Table/types';

import { useMcpServerActions } from '@app/settingsV2/platform/ai/mcpServers/hooks/useMcpServerActions';
import {
    type McpServerRow,
    transformServersToRows,
} from '@app/settingsV2/platform/ai/mcpServers/utils/mcpServerDataUtils';
import { ConfirmationModal } from '@app/sharedV2/modals/ConfirmationModal';
import { Alert, Button, Pill, Switch, Table, Text, toast } from '@src/alchemy-components';

import { GetMcpServersDocument, useGetMcpServersQuery } from '@graphql/mcpServers.generated';

const Container = styled.div`
    display: flex;
    flex-direction: column;
    flex: 1;
    overflow: auto;
    padding-top: 16px;
    min-height: 0;
    gap: 16px;
`;

// Full-width card holding the master on/off switch: title + description on the
// left, the toggle on the right.
const MasterSwitchCard = styled.div`
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 16px;
    flex-shrink: 0;
    padding: 16px 20px;
    border: 1px solid ${(props) => props.theme.colors.border};
    border-radius: 12px;
`;

const MasterSwitchText = styled.div`
    display: flex;
    flex-direction: column;
    gap: 2px;
    /* Set the semantic text color here so the alchemy <Text> title (color="inherit")
       picks it up instead of falling back to the browser default. */
    color: ${(props) => props.theme.colors.text};
`;

const TableSection = styled.div`
    flex-shrink: 0;
`;

const NameCell = styled.div`
    display: flex;
    align-items: center;
    gap: 8px;
    min-width: 0;
    font-size: 14px;
    font-weight: 600;
    color: ${(props) => props.theme.colors.text};
`;

const UrlCell = styled.div`
    display: flex;
    align-items: center;
    gap: 6px;
    min-width: 0;
    font-family: monospace;
    font-size: 12px;
`;

const UrlText = styled.span`
    min-width: 0;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
`;

export const McpServersTab = () => {
    const { t } = useTranslation('settings.ai');
    const { t: tc } = useTranslation('common.actions');
    // One server's nested field (say, a View the viewer can't see) can fail; errorPolicy 'all'
    // keeps the rest of the list instead of discarding the whole result.
    const { data, loading, error, refetch } = useGetMcpServersQuery({ errorPolicy: 'all' });
    const client = useApolloClient();
    const { handleToggleEnabled, handleToggleGlobalEnabled } = useMcpServerActions({ onSuccess: refetch });

    // Turning the master switch OFF kills every MCP server, so it's gated behind a
    // confirmation. Turning it back on is not.
    const [confirmingDisable, setConfirmingDisable] = useState(false);

    const mcpSettings = data?.mcpSettings;
    const globalEnabled = mcpSettings?.enabled ?? true;
    const rows = transformServersToRows(mcpSettings?.servers, mcpSettings?.defaultMcpServerOverrides);

    const handleCopyUrl = (url: string) => {
        navigator.clipboard?.writeText(url);
        toast.success(t('platform.ai.mcpServers.urlCopied'));
    };

    const columns: Column<McpServerRow>[] = [
        {
            title: t('platform.ai.mcpServers.columnName'),
            key: 'name',
            render: (record) => (
                <NameCell data-testid={`mcp-server-name-${record.id}`}>
                    {record.name}
                    {record.isDefault && <Pill label={t('platform.ai.mcpServers.defaultTag')} color="gray" size="sm" />}
                </NameCell>
            ),
        },
        {
            title: t('platform.ai.mcpServers.columnConnectionUrl'),
            key: 'connectionUrl',
            render: (record) => (
                <UrlCell>
                    <UrlText title={record.connectionUrl}>
                        <Text color="gray" type="span">
                            {record.connectionUrl}
                        </Text>
                    </UrlText>
                    <Button
                        variant="text"
                        size="sm"
                        onClick={() => handleCopyUrl(record.connectionUrl)}
                        icon={{ icon: Copy }}
                        aria-label={t('platform.ai.mcpServers.copyUrl')}
                    />
                </UrlCell>
            ),
        },
        {
            title: t('platform.ai.mcpServers.columnEnabled'),
            key: 'enabled',
            width: '88px',
            alignment: 'center',
            render: (record) => (
                <Switch
                    label=""
                    isChecked={record.enabled}
                    onChange={() => handleToggleEnabled(record)}
                    data-testid={`mcp-server-toggle-${record.id}`}
                />
            ),
        },
    ];

    // Without settings the switch and rows would fall back to defaults (every switch on, only the
    // default server), which reads as real state. Show the failure instead.
    if (error && !mcpSettings) {
        return (
            <Container data-testid="mcp-servers-tab">
                <Alert
                    variant="error"
                    title={t('platform.ai.mcpServers.loadError')}
                    // Refetch every active copy of the query, the settings header's included.
                    action={{
                        label: tc('retry'),
                        onClick: () => client.refetchQueries({ include: [GetMcpServersDocument] }),
                    }}
                />
            </Container>
        );
    }

    return (
        <Container data-testid="mcp-servers-tab">
            {/* A failed field reads as empty (a View as "View unavailable"), so say that the data is partial. */}
            {error && <Alert variant="warning" title={t('platform.ai.mcpServers.partialLoadError')} />}
            <MasterSwitchCard>
                <MasterSwitchText>
                    <Text weight="bold" size="lg">
                        {t('platform.ai.mcpServers.masterSwitchLabel')}
                    </Text>
                    <Text color="gray">{t('platform.ai.mcpServers.masterSwitchHelp')}</Text>
                </MasterSwitchText>
                <Switch
                    label=""
                    isChecked={globalEnabled}
                    onChange={(e) => {
                        // Enabling applies immediately; disabling asks first.
                        if (e.target.checked) handleToggleGlobalEnabled(true);
                        else setConfirmingDisable(true);
                    }}
                    data-testid="mcp-master-switch"
                />
            </MasterSwitchCard>

            {globalEnabled && (
                <TableSection data-testid="mcp-servers-table">
                    <Table<McpServerRow> columns={columns} data={rows} isLoading={loading} showHeader rowKey="id" />
                </TableSection>
            )}

            <ConfirmationModal
                isOpen={confirmingDisable}
                handleClose={() => setConfirmingDisable(false)}
                handleConfirm={() => {
                    setConfirmingDisable(false);
                    handleToggleGlobalEnabled(false);
                }}
                modalTitle={t('platform.ai.mcpServers.masterSwitchDisableTitle')}
                modalText={t('platform.ai.mcpServers.masterSwitchDisableBody')}
                confirmButtonText={t('platform.ai.mcpServers.masterSwitchDisableConfirm')}
                isDeleteModal
            />
        </Container>
    );
};
