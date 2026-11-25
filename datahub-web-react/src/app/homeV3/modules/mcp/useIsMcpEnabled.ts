import { useIsMcpServerEnabled } from '@app/useAppConfig';

import { useGetMcpEnabledQuery } from '@graphql/mcpServers.generated';

/**
 * Whether DataHub's MCP server is on: the feature is on (MCP_SERVER_ENABLED) and an admin has left
 * it on (Settings → AI). Loading or a failed settings read counts as on, so the home page MCP
 * module doesn't flash away on every load.
 */
export function useIsMcpEnabled({ skip = false }: { skip?: boolean } = {}) {
    const mcpServerEnabled = useIsMcpServerEnabled();
    const { data, loading } = useGetMcpEnabledQuery({
        fetchPolicy: 'cache-first',
        skip: skip || !mcpServerEnabled,
    });
    return { isMcpEnabled: mcpServerEnabled && data?.mcpSettings?.enabled !== false, loading };
}
