import { render, screen } from '@testing-library/react';
import React from 'react';
import { describe, expect, it, vi } from 'vitest';

import PlatformsModule from '@app/homeV3/modules/platforms/PlatformsModule';

// More than the former hard cap of 15 so we can assert the long tail still renders.
const NUM_PLATFORMS = 20;

vi.mock('@app/homeV2/content/tabs/discovery/sections/platform/useGetPlatforms', () => ({
    useGetPlatforms: () => ({
        loading: false,
        platforms: Array.from({ length: NUM_PLATFORMS }, (_, i) => ({
            count: NUM_PLATFORMS - i,
            platform: { urn: `urn:li:dataPlatform:platform-${i}` },
        })),
    }),
}));

vi.mock('@app/homeV3/modules/platforms/usePlatformsModuleUtils', () => ({
    default: () => ({ navigateToDataSources: vi.fn(), handleEntityClick: vi.fn() }),
}));

vi.mock('@app/homeV3/module/components/LargeModule', () => ({
    default: ({ children }: any) => <div data-testid="large-module">{children}</div>,
}));

vi.mock('@app/homeV3/module/components/EmptyContent', () => ({
    default: () => <div data-testid="empty-content" />,
}));

vi.mock('@app/homeV3/module/components/EntityItem', () => ({
    default: ({ entity }: any) => <div data-testid="platform-item">{entity.urn}</div>,
}));

vi.mock('@app/context/useUserContext', () => ({
    useUserContext: () => ({ platformPrivileges: {} }),
}));

vi.mock('@app/useAppConfig', () => ({
    useAppConfig: () => ({ config: { managedIngestionConfig: { enabled: false } } }),
}));

vi.mock('@components', () => ({
    Text: ({ children }: any) => <span>{children}</span>,
    Tooltip: ({ children }: any) => <>{children}</>,
}));

vi.mock('@phosphor-icons/react/dist/csr/Database', () => ({ Database: () => <svg /> }));

vi.mock('react-i18next', () => ({
    useTranslation: () => ({ t: (key: string) => key }),
}));

describe('PlatformsModule', () => {
    it('renders every platform the backend returns, not just the first 15', () => {
        render(<PlatformsModule {...({} as any)} />);

        const items = screen.getAllByTestId('platform-item');
        expect(items).toHaveLength(NUM_PLATFORMS);
        // Long-tail platforms that the former slice(0, 15) would have dropped are present.
        expect(screen.getByText('urn:li:dataPlatform:platform-15')).toBeInTheDocument();
        expect(screen.getByText('urn:li:dataPlatform:platform-19')).toBeInTheDocument();
    });
});
