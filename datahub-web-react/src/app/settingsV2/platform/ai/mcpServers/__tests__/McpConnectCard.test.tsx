import { fireEvent, render, screen } from '@testing-library/react';
import React from 'react';
import { BrowserRouter, MemoryRouter } from 'react-router-dom';
import { afterEach, describe, expect, it, vi } from 'vitest';

import { McpConnectCard } from '@app/settingsV2/platform/ai/mcpServers/McpConnectCard';
import CustomThemeProvider from '@src/CustomThemeProvider';
import { resetCachedBasePath } from '@utils/runtimeBasePath';

const mockAuthConfig = { tokenAuthEnabled: true };
vi.mock('@app/useAppConfig', async (importOriginal) => ({
    ...(await importOriginal<object>()),
    useAppConfig: () => ({ config: { authConfig: mockAuthConfig } }),
}));

function renderCard(router: 'memory' | 'browser' = 'memory') {
    const card = (
        <CustomThemeProvider>
            <McpConnectCard />
        </CustomThemeProvider>
    );
    // The app's router takes the base path as its basename.
    return render(
        router === 'browser' ? (
            <BrowserRouter basename="/datahub">{card}</BrowserRouter>
        ) : (
            <MemoryRouter>{card}</MemoryRouter>
        ),
    );
}

const snippet = () => screen.getByTestId('mcp-connect-snippet').textContent ?? '';

describe('McpConnectCard', () => {
    afterEach(() => {
        mockAuthConfig.tokenAuthEnabled = true;
        document.querySelector('base')?.remove();
        resetCachedBasePath();
        window.location.pathname = '/';
    });

    it('sets clients up with an access token when token auth is on', () => {
        renderCard();
        const url = `${window.location.origin}/mcp`;

        expect(screen.getByTestId('mcp-connect-token-link')).toHaveAttribute('href', '/settings/tokens');
        expect(screen.queryByTestId('mcp-connect-no-auth')).not.toBeInTheDocument();
        expect(snippet()).toContain(`claude mcp add --transport http datahub ${url}`);
        expect(snippet()).toContain('--header "Authorization: Bearer <your-access-token>"');

        fireEvent.click(screen.getByTestId('mcp-connect-client-cursor'));
        expect(JSON.parse(snippet())).toEqual({
            mcpServers: { datahub: { url, headers: { Authorization: 'Bearer <your-access-token>' } } },
        });

        fireEvent.click(screen.getByTestId('mcp-connect-client-claude-desktop'));
        expect(snippet()).toContain('mcp-remote');
        expect(screen.getByText(/runs the mcp-remote bridge on your machine/)).toBeInTheDocument();
    });

    it('sets clients up without a token, and warns that calls run as the system user, when token auth is off', () => {
        mockAuthConfig.tokenAuthEnabled = false;
        renderCard();

        expect(screen.getByTestId('mcp-connect-no-auth')).toHaveTextContent("run as DataHub's system user");
        expect(screen.queryByTestId('mcp-connect-token-link')).not.toBeInTheDocument();
        expect(snippet()).toBe(`claude mcp add --transport http datahub ${window.location.origin}/mcp`);

        fireEvent.click(screen.getByTestId('mcp-connect-client-vscode'));
        expect(JSON.parse(snippet())).toEqual({
            servers: { datahub: { type: 'http', url: `${window.location.origin}/mcp` } },
        });
    });

    it('keeps the base path DataHub is served under in the URL and the token link', () => {
        // setupTests swaps window.location for a plain object, so set the path directly.
        window.location.pathname = '/datahub/settings/ai/mcp-servers';
        const base = document.createElement('base');
        base.setAttribute('href', '/datahub/');
        document.head.appendChild(base);
        resetCachedBasePath();
        renderCard('browser');

        expect(snippet()).toContain(`${window.location.origin}/datahub/mcp`);
        expect(screen.getByTestId('mcp-connect-token-link')).toHaveAttribute('href', '/datahub/settings/tokens');
    });
});
