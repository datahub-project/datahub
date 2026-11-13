import { fireEvent, render, screen } from '@testing-library/react';
import React from 'react';
import { BrowserRouter, MemoryRouter } from 'react-router-dom';
import { afterEach, describe, expect, it, vi } from 'vitest';

import { McpConnectCard } from '@app/settingsV2/platform/ai/mcpServers/McpConnectCard';
import { isPublicHttpsUrl } from '@app/settingsV2/platform/ai/mcpServers/McpConnectCard.utils';
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

const ORIGIN = window.location.origin;

const snippet = () => screen.getByTestId('mcp-connect-snippet').textContent ?? '';

describe('McpConnectCard', () => {
    afterEach(() => {
        mockAuthConfig.tokenAuthEnabled = true;
        document.querySelector('base')?.remove();
        resetCachedBasePath();
        window.location.pathname = '/';
        (window.location as { origin: string }).origin = ORIGIN;
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

    it('walks through adding a Claude connector with the token as a request header on a public HTTPS URL', () => {
        (window.location as { origin: string }).origin = 'https://datahub.example.com';
        renderCard();
        fireEvent.click(screen.getByTestId('mcp-connect-client-claude-connector'));

        expect(screen.getByTestId('mcp-connect-connector-recommended')).toBeInTheDocument();
        expect(screen.queryByTestId('mcp-connect-connector-private')).not.toBeInTheDocument();
        expect(screen.getByTestId('mcp-connect-connector-url')).toHaveTextContent('https://datahub.example.com/mcp');
        expect(screen.getByTestId('mcp-connect-connector-header')).toHaveTextContent('Bearer <your-access-token>');
        expect(screen.getAllByText('authorization').length).toBeGreaterThan(0);
        expect(screen.getByTestId('mcp-connect-connector-headers-beta')).toBeInTheDocument();
    });

    it('adds the connector without a header when token auth is off', () => {
        (window.location as { origin: string }).origin = 'https://datahub.example.com';
        mockAuthConfig.tokenAuthEnabled = false;
        renderCard();
        fireEvent.click(screen.getByTestId('mcp-connect-client-claude-connector'));

        expect(screen.queryByTestId('mcp-connect-connector-header')).not.toBeInTheDocument();
        expect(screen.getByText('No sign-in')).toBeInTheDocument();
    });

    it('points to the local bridge when Anthropic cannot reach the URL', () => {
        (window.location as { origin: string }).origin = 'http://localhost:9002';
        renderCard();
        fireEvent.click(screen.getByTestId('mcp-connect-client-claude-connector'));

        expect(screen.getByTestId('mcp-connect-connector-private')).toBeInTheDocument();
        fireEvent.click(screen.getAllByTestId('mcp-connect-use-local')[0]);
        expect(snippet()).toContain('mcp-remote');
    });

    it("explains that ChatGPT can't connect, without steps, when token auth is on", () => {
        (window.location as { origin: string }).origin = 'https://datahub.example.com';
        renderCard();
        fireEvent.click(screen.getByTestId('mcp-connect-client-chatgpt'));

        expect(screen.getByTestId('mcp-connect-chatgpt-token-auth')).toBeInTheDocument();
        expect(screen.queryByTestId('mcp-connect-chatgpt-url')).not.toBeInTheDocument();
        fireEvent.click(screen.getByTestId('mcp-connect-chatgpt-use-claude'));
        expect(screen.getByTestId('mcp-connect-connector-url')).toHaveTextContent('https://datahub.example.com/mcp');
    });

    it('walks through adding ChatGPT with no authentication, and warns the URL is open, on a public HTTPS URL with token auth off', () => {
        (window.location as { origin: string }).origin = 'https://datahub.example.com';
        mockAuthConfig.tokenAuthEnabled = false;
        renderCard();
        fireEvent.click(screen.getByTestId('mcp-connect-client-chatgpt'));

        expect(screen.getByTestId('mcp-connect-no-auth')).toBeInTheDocument();
        expect(screen.getByTestId('mcp-connect-chatgpt-public-no-auth')).toBeInTheDocument();
        expect(screen.getByTestId('mcp-connect-chatgpt-url')).toHaveTextContent('https://datahub.example.com/mcp');
        expect(screen.getByText('No authentication')).toBeInTheDocument();
    });

    it("says ChatGPT can't reach a private URL with token auth off", () => {
        (window.location as { origin: string }).origin = 'http://localhost:9002';
        mockAuthConfig.tokenAuthEnabled = false;
        renderCard();
        fireEvent.click(screen.getByTestId('mcp-connect-client-chatgpt'));

        expect(screen.getByTestId('mcp-connect-chatgpt-private')).toBeInTheDocument();
        expect(screen.queryByTestId('mcp-connect-chatgpt-url')).not.toBeInTheDocument();
        fireEvent.click(screen.getByTestId('mcp-connect-chatgpt-use-cursor'));
        expect(screen.getByText('Add to ~/.cursor/mcp.json')).toBeInTheDocument();
    });
});

describe('isPublicHttpsUrl', () => {
    it('accepts HTTPS URLs on public host names and addresses', () => {
        expect(isPublicHttpsUrl('https://datahub.example.com/mcp')).toBe(true);
        expect(isPublicHttpsUrl('https://8.8.8.8/mcp')).toBe(true);
    });

    it('rejects plain HTTP, localhost, private addresses and local-only names', () => {
        expect(isPublicHttpsUrl('http://datahub.example.com/mcp')).toBe(false);
        expect(isPublicHttpsUrl('https://localhost:9002/mcp')).toBe(false);
        expect(isPublicHttpsUrl('https://10.0.0.5/mcp')).toBe(false);
        expect(isPublicHttpsUrl('https://192.168.1.20/mcp')).toBe(false);
        expect(isPublicHttpsUrl('https://172.20.0.1/mcp')).toBe(false);
        expect(isPublicHttpsUrl('https://[::1]/mcp')).toBe(false);
        expect(isPublicHttpsUrl('https://datahub.local/mcp')).toBe(false);
        expect(isPublicHttpsUrl('https://datahub/mcp')).toBe(false);
    });
});
