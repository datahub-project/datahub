import { useCallback } from 'react';

import { usePageTemplateContext } from '@app/homeV3/context/PageTemplateContext';
import { useIsMcpEnabled } from '@app/homeV3/modules/mcp/useIsMcpEnabled';
import { useIsMcpServerEnabled } from '@app/useAppConfig';

import { PageModuleFragment } from '@graphql/template.generated';
import { DataHubPageModuleType, PageTemplateSurfaceType } from '@types';

/**
 * Modules that take no space on the page in the current state. They keep their place in the
 * template, so module positions (and every move, add, and remove) still count them.
 *
 * The MCP module is hidden while MCP is turned off. Admins editing the org default still see it,
 * as a placeholder they can remove, unless the MCP server feature itself is off
 * (MCP_SERVER_ENABLED): then it is hidden for everyone.
 */
export function useIsModuleHidden(): (module: PageModuleFragment) => boolean {
    const { templateType, isEditingGlobalTemplate } = usePageTemplateContext();
    const { isMcpEnabled } = useIsMcpEnabled({ skip: templateType !== PageTemplateSurfaceType.HomePage });
    const mcpServerEnabled = useIsMcpServerEnabled();

    return useCallback(
        (module: PageModuleFragment) =>
            module.properties.type === DataHubPageModuleType.Mcp &&
            (!mcpServerEnabled || (!isMcpEnabled && !isEditingGlobalTemplate)),
        [mcpServerEnabled, isMcpEnabled, isEditingGlobalTemplate],
    );
}
