import { render, screen } from '@testing-library/react';
import React from 'react';
import { describe, expect, it, vi } from 'vitest';

import { PluginLogo } from '@app/settingsV2/platform/ai/plugins/components/PluginLogo';

// Mock the logo utils
vi.mock('@app/settingsV2/platform/ai/plugins/utils/pluginLogoUtils', () => ({
    getPluginLogoUrl: vi.fn((displayName: string) => {
        if (displayName.toLowerCase().includes('github')) {
            return '/mock-github-logo.png';
        }
        return null;
    }),
}));

describe('PluginLogo', () => {
    it('renders fallback icon when no logo URL is found', () => {
        render(<PluginLogo displayName="Unknown Plugin" url={null} />);

        // Should render SVG fallback (Plug icon)
        const svg = document.querySelector('svg');
        expect(svg).toBeInTheDocument();
    });

    it('renders logo image when URL is found', () => {
        render(<PluginLogo displayName="GitHub Plugin" url="https://github.com" />);

        const img = screen.getByRole('img', { name: 'Plugin logo' });
        expect(img).toBeInTheDocument();
        expect(img).toHaveAttribute('src', '/mock-github-logo.png');
    });
});
