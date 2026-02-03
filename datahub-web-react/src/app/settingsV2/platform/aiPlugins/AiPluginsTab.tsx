import { DotsThreeVertical } from '@phosphor-icons/react';
import React, { useEffect, useState } from 'react';
import styled from 'styled-components';

import { Menu } from '@components/components/Menu/Menu';
import { Column } from '@components/components/Table/types';

import CreatePluginModal from '@app/settingsV2/platform/aiPlugins/CreatePluginModal';
import { PluginLogo } from '@app/settingsV2/platform/aiPlugins/components/PluginLogo';
import { usePluginActions } from '@app/settingsV2/platform/aiPlugins/hooks/usePluginActions';
import {
    AiPluginRow,
    createDuplicatePlugin,
    transformPluginsToRows,
} from '@app/settingsV2/platform/aiPlugins/utils/pluginDataUtils';
import { getAuthTypeLabel } from '@app/settingsV2/platform/aiPlugins/utils/pluginDisplayUtils';
import { Button, Switch, Table, Text, colors } from '@src/alchemy-components';
import { useGetAiPluginsQuery } from '@src/graphql/aiPlugins.generated';
import EmptyFormsImage from '@src/images/empty-forms.svg?react';
import { AiPluginConfig } from '@src/types.generated';

type AiPluginsTabProps = {
    showCreateModal: boolean;
    setShowCreateModal: (show: boolean) => void;
};

const Container = styled.div`
    display: flex;
    flex-direction: column;
    flex: 1;
    overflow: hidden;
    padding-top: 16px;
    min-height: 0;
`;

const TableSection = styled.div`
    flex: 1;
    overflow: hidden;
    min-height: 0;
`;

const EmptyContainer = styled.div`
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    height: 100%;
    width: 100%;
    padding: 60px 20px;
    gap: 16px;

    svg {
        width: 160px;
        height: auto;
    }
`;

const ActionsCell = styled.div`
    display: flex;
    justify-content: flex-end;
`;

const NameCell = styled.div`
    display: flex;
    align-items: center;
    gap: 12px;
`;

/**
 * AI Plugins tab content.
 * Displays a table of configured AI plugins with their status.
 *
 * TODO(Alex): Add ownership field to GetAiPlugins query to support "Created By" column.
 */
export const AiPluginsTab = ({ showCreateModal, setShowCreateModal }: AiPluginsTabProps) => {
    const { data, loading, refetch } = useGetAiPluginsQuery();
    const { handleToggleEnabled, handleDelete } = usePluginActions({ onSuccess: refetch });

    const [editingPlugin, setEditingPlugin] = useState<AiPluginConfig | null>(null);

    const plugins = data?.globalSettings?.aiPlugins;
    const rows = transformPluginsToRows(plugins as AiPluginConfig[] | undefined);

    // Determine if modal should be open (either create or edit)
    const isModalOpen = showCreateModal || editingPlugin !== null;

    // Reset editingPlugin when create modal is opened from header
    useEffect(() => {
        if (showCreateModal) {
            setEditingPlugin(null);
        }
    }, [showCreateModal]);

    const handleEdit = (pluginRow: AiPluginRow) => {
        setEditingPlugin(pluginRow.plugin);
    };

    const handleDuplicate = (pluginRow: AiPluginRow) => {
        const duplicatedPlugin = createDuplicatePlugin(pluginRow);
        setEditingPlugin(duplicatedPlugin);
    };

    const handleCloseModal = () => {
        setShowCreateModal(false);
        setEditingPlugin(null);
        refetch();
    };

    const getActionsMenuItems = (record: AiPluginRow) => [
        {
            type: 'item' as const,
            key: 'edit',
            title: 'Edit',
            icon: 'PencilSimple' as const,
            onClick: () => handleEdit(record),
        },
        {
            type: 'item' as const,
            key: 'duplicate',
            title: 'Duplicate',
            icon: 'Copy' as const,
            onClick: () => handleDuplicate(record),
        },
        {
            type: 'item' as const,
            key: 'delete',
            title: 'Delete',
            icon: 'Trash' as const,
            danger: true,
            onClick: () => handleDelete(record),
        },
    ];

    const columns: Column<AiPluginRow>[] = [
        {
            title: 'Plugin Name',
            key: 'name',
            sorter: (a, b) => a.name.localeCompare(b.name),
            render: (record) => (
                <NameCell>
                    <PluginLogo displayName={record.name} url={record.url} />
                    <Text weight="semiBold">{record.name}</Text>
                </NameCell>
            ),
        },
        {
            title: 'Type',
            key: 'authType',
            width: '150px',
            render: (record) => <Text color="gray">{getAuthTypeLabel(record.authType)}</Text>,
        },
        {
            title: 'Enabled',
            key: 'enabled',
            width: '100px',
            alignment: 'center',
            render: (record) => (
                <Switch label="" isChecked={record.enabled} onChange={() => handleToggleEnabled(record)} />
            ),
        },
        {
            title: '',
            key: 'actions',
            width: '60px',
            alignment: 'right',
            render: (record) => (
                <ActionsCell onClick={(e) => e.stopPropagation()}>
                    <Menu items={getActionsMenuItems(record)} trigger={['click']}>
                        <Button variant="text" size="sm" data-testid={`plugin-actions-${record.id}`}>
                            <DotsThreeVertical size={20} color={colors.gray[500]} weight="bold" />
                        </Button>
                    </Menu>
                </ActionsCell>
            ),
        },
    ];

    if (!loading && rows.length === 0) {
        return (
            <Container data-testid="ai-plugins-tab-empty">
                <EmptyContainer>
                    <EmptyFormsImage />
                    <Text size="lg" color="gray" weight="bold">
                        No plugins yet!
                    </Text>
                </EmptyContainer>

                {isModalOpen && (
                    <CreatePluginModal
                        editingPlugin={editingPlugin}
                        onClose={handleCloseModal}
                        existingNames={rows.map((r) => r.name)}
                    />
                )}
            </Container>
        );
    }

    return (
        <Container data-testid="ai-plugins-tab">
            <TableSection data-testid="ai-plugins-table">
                <Table<AiPluginRow>
                    columns={columns}
                    data={rows}
                    isLoading={loading}
                    showHeader
                    rowKey="id"
                    isScrollable
                />
            </TableSection>

            {isModalOpen && (
                <CreatePluginModal
                    editingPlugin={editingPlugin}
                    onClose={handleCloseModal}
                    existingNames={rows.map((r) => r.name)}
                />
            )}
        </Container>
    );
};
