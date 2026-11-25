import { Modal } from '@components';
import React from 'react';
import { useTranslation } from 'react-i18next';

import { McpConnectSetup } from '@app/settingsV2/platform/ai/mcpServers/McpConnectCard';
import type { McpClient } from '@app/settingsV2/platform/ai/mcpServers/McpConnectCard.utils';

type Props = {
    /** The client button that opened the modal: its name (none for "Other"), and the setup tab to start on. */
    name?: string;
    client: McpClient;
    onClose: () => void;
};

/** The settings page's Connect setup, opened from the home page module at one client's tab. */
export default function McpClientSetupModal({ name, client, onClose }: Props) {
    const { t } = useTranslation('modules');

    return (
        <Modal
            open
            title={name ? t('mcp.setupTitle', { client: name }) : t('mcp.setupTitleOther')}
            onCancel={onClose}
            width={720}
            dataTestId="mcp-setup-modal"
        >
            <McpConnectSetup initialClient={client} />
        </Modal>
    );
}
