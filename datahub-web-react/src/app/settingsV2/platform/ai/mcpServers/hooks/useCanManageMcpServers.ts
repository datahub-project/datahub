import { useUserContext } from '@app/context/useUserContext';
import { useIsMcpServerEnabled } from '@app/useAppConfig';

/**
 * Whether the current user can see and manage DataHub's MCP servers: the MCP server is on
 * (MCP_SERVER_ENABLED), and the user has the Manage Platform Settings privilege that the settings
 * mutation requires.
 */
export function useCanManageMcpServers(): boolean {
    const me = useUserContext();
    const mcpServerEnabled = useIsMcpServerEnabled();
    return mcpServerEnabled && !!me?.platformPrivileges?.manageGlobalSettings;
}
