import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import React from 'react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { AIChatButton } from '@app/aiAssistant/AIChatButton';
import CustomThemeProvider from '@src/CustomThemeProvider';

vi.mock('@utils/runtimeBasePath', () => ({
    resolveRuntimePath: (path: string) => path,
}));

const renderComponent = () =>
    render(
        <CustomThemeProvider>
            <AIChatButton />
        </CustomThemeProvider>,
    );

describe('AIChatButton', () => {
    beforeEach(() => {
        Object.defineProperty(window.HTMLElement.prototype, 'scrollIntoView', {
            value: vi.fn(),
            writable: true,
        });
    });

    afterEach(() => {
        vi.unstubAllGlobals();
    });

    it('loads chat model options from the backend', async () => {
        const fetchMock = vi.fn().mockResolvedValue({
            ok: true,
            json: async () => ({ models: ['SONNET', 'OPUS', 'GPT_5_5'] }),
        });

        vi.stubGlobal('fetch', fetchMock);

        renderComponent();
        fireEvent.click(screen.getByTitle('Open AI Assistant'));

        expect(await screen.findByRole('option', { name: 'Claude Sonnet 5' })).toBeInTheDocument();
        expect(screen.getByRole('option', { name: 'Claude Opus 4.8' })).toBeInTheDocument();
        expect(screen.getByRole('option', { name: 'GPT 5.5' })).toBeInTheDocument();

        await waitFor(() => {
            expect(fetchMock).toHaveBeenCalledWith('/api/ai-config/models');
        });
    });
});
