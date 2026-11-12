import { afterEach, describe, expect, it } from 'vitest';

import {
    DEFAULT_SERVER_DISPLAY_LABEL,
    buildConnectionUrl,
    transformServersToRows,
} from '@app/settingsV2/platform/ai/mcpServers/utils/mcpServerDataUtils';
import { resetCachedBasePath } from '@utils/runtimeBasePath';

import type { McpServerConfigFieldsFragment } from '@graphql/mcpServers.generated';

const auditStamp = { __typename: 'AuditStamp' as const, time: 0 };

function server(overrides: Partial<McpServerConfigFieldsFragment> = {}): McpServerConfigFieldsFragment {
    return {
        __typename: 'McpServerConfig',
        slug: 'finance',
        name: 'Finance',
        enabled: true,
        created: auditStamp,
        lastModified: auditStamp,
        ...overrides,
    };
}

function setBaseHref(href: string | null) {
    document.querySelector('base')?.remove();
    if (href) {
        const base = document.createElement('base');
        base.setAttribute('href', href);
        document.head.appendChild(base);
    }
    resetCachedBasePath();
}

describe('buildConnectionUrl', () => {
    afterEach(() => setBaseHref(null));

    it('serves the default server at /mcp and a scoped server at /mcp/<slug>', () => {
        const { origin } = window.location;
        expect(buildConnectionUrl()).toBe(`${origin}/mcp`);
        expect(buildConnectionUrl('finance')).toBe(`${origin}/mcp/finance`);
    });

    it('keeps the base path DataHub is served under', () => {
        setBaseHref('/datahub/');
        expect(buildConnectionUrl()).toBe(`${window.location.origin}/datahub/mcp`);
    });
});

describe('transformServersToRows', () => {
    it('pins the default server first and sorts scoped servers by name', () => {
        const rows = transformServersToRows(
            [server({ slug: 'b', name: 'Beta' }), server({ slug: 'a', name: 'Alpha' })],
            null,
        );

        expect(rows.map((row) => row.name)).toEqual([DEFAULT_SERVER_DISPLAY_LABEL, 'Alpha', 'Beta']);
        expect(rows[0]).toMatchObject({ isDefault: true, enabled: true, slug: undefined });
    });

    it('takes the default server name and state from its overrides', () => {
        const [row] = transformServersToRows([], server({ slug: null, name: 'Company MCP', enabled: false }));

        expect(row).toMatchObject({ isDefault: true, name: 'Company MCP', enabled: false });
    });

    it("marks a View that doesn't resolve as unavailable", () => {
        const rows = transformServersToRows([server({ viewUrn: 'urn:li:dataHubView:gone', view: null })], null);

        expect(rows[1].viewUnavailable).toBe(true);
    });
});
