import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import React from 'react';
import { afterEach, describe, expect, it, vi } from 'vitest';

import { AIAssistantSettings } from '@app/settingsV2/AIAssistantSettings';
import CustomThemeProvider from '@src/CustomThemeProvider';

vi.mock('@utils/runtimeBasePath', () => ({
    resolveRuntimePath: (path: string) => path,
}));

const renderComponent = () =>
    render(
        <CustomThemeProvider>
            <AIAssistantSettings />
        </CustomThemeProvider>,
    );

describe('AIAssistantSettings', () => {
    afterEach(() => {
        vi.unstubAllGlobals();
    });

    it('loads providers and saved key state from the backend', async () => {
        const fetchMock = vi
            .fn()
            .mockResolvedValueOnce({
                ok: true,
                json: async () => ({ providers: ['CLAUDE', 'OPENAI'] }),
            })
            .mockResolvedValueOnce({
                ok: true,
                json: async () => ({ models: ['SONNET', 'GPT_5_5'] }),
            })
            .mockResolvedValueOnce({
                ok: true,
                json: async () => ({
                    provider: 'claude',
                    hasKey: true,
                    updated: false,
                    keyPreview: 'sk-ant-...7890',
                }),
            });

        vi.stubGlobal('fetch', fetchMock);

        renderComponent();

        expect(
            await screen.findByRole('option', { name: 'Anthropic (Claude)' }),
        ).toBeInTheDocument();
        expect(await screen.findByText('Saved key on file: sk-ant-...7890')).toBeInTheDocument();
        expect(
            screen.getByText(/Supported models from GMS: Claude Sonnet, GPT 5\.5\./),
        ).toBeInTheDocument();

        await waitFor(() => {
            expect(fetchMock).toHaveBeenNthCalledWith(1, '/api/ai-config/providers');
            expect(fetchMock).toHaveBeenNthCalledWith(2, '/api/ai-config/models');
            expect(fetchMock).toHaveBeenNthCalledWith(
                3,
                '/api/ai-config/api-key?provider=claude',
            );
        });
    });

    it('saves a provider key and refreshes the saved preview', async () => {
        const fetchMock = vi
            .fn()
            .mockResolvedValueOnce({
                ok: true,
                json: async () => ({ providers: ['CLAUDE'] }),
            })
            .mockResolvedValueOnce({
                ok: true,
                json: async () => ({ models: ['SONNET'] }),
            })
            .mockResolvedValueOnce({
                ok: true,
                json: async () => ({
                    provider: 'claude',
                    hasKey: false,
                    updated: false,
                }),
            })
            .mockResolvedValueOnce({
                ok: true,
                json: async () => ({
                    provider: 'claude',
                    hasKey: true,
                    updated: true,
                }),
            })
            .mockResolvedValueOnce({
                ok: true,
                json: async () => ({
                    provider: 'claude',
                    hasKey: true,
                    updated: false,
                    keyPreview: 'sk-ant-...1234',
                }),
            });

        vi.stubGlobal('fetch', fetchMock);

        renderComponent();

        const input = await screen.findByLabelText('API Key');
        fireEvent.change(input, { target: { value: 'sk-ant-test-1234567890' } });
        fireEvent.click(screen.getByRole('button', { name: 'Save Configuration' }));

        await waitFor(() => {
            expect(fetchMock).toHaveBeenNthCalledWith(4, '/api/ai-config/api-key', {
                method: 'PUT',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    provider: 'claude',
                    apiKey: 'sk-ant-test-1234567890',
                }),
            });
        });

        expect(await screen.findByText('Configuration saved')).toBeInTheDocument();
        expect(await screen.findByText('Saved key on file: sk-ant-...1234')).toBeInTheDocument();
    });
});
