import { Globe } from '@phosphor-icons/react/dist/csr/Globe';
import { Lock } from '@phosphor-icons/react/dist/csr/Lock';
import { WarningCircle } from '@phosphor-icons/react/dist/csr/WarningCircle';
import React from 'react';
import { useTranslation } from 'react-i18next';

import { type McpServerRow } from '@app/settingsV2/platform/ai/mcpServers/utils/mcpServerDataUtils';
import { Pill } from '@src/alchemy-components';

import { DataHubViewType } from '@types';

interface Props {
    server: McpServerRow;
}

/** A server's scope: its View (lock for personal, globe for global), or All Data. */
export const McpServerScopePill: React.FC<Props> = ({ server }) => {
    const { t } = useTranslation('settings.ai');
    // Not "All Data": the server still names a View, so an admin has to pick another one.
    if (server.viewUnavailable) {
        return (
            <Pill
                label={t('platform.ai.mcpServers.viewUnavailable')}
                leftIcon={WarningCircle}
                color="red"
                variant="outline"
                size="md"
            />
        );
    }
    if (server.viewName) {
        return (
            <Pill
                label={server.viewName}
                leftIcon={server.viewType === DataHubViewType.Personal ? Lock : Globe}
                color="gray"
                variant="outline"
                size="md"
            />
        );
    }
    return <Pill label={t('platform.ai.mcpServers.noView')} color="gray" variant="outline" size="md" />;
};
