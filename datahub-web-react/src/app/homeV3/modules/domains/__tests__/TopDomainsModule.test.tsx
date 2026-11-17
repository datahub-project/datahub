import { render, waitFor } from '@testing-library/react';
import React from 'react';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { useGetDomains } from '@app/homeV2/content/tabs/discovery/sections/domains/useGetDomains';
import { useModuleContext } from '@app/homeV3/module/context/ModuleContext';
import TopDomainsModule from '@app/homeV3/modules/domains/TopDomainsModule';

vi.mock('@app/homeV2/content/tabs/discovery/sections/domains/useGetDomains', () => ({
    useGetDomains: vi.fn(),
}));

vi.mock('@app/homeV3/module/context/ModuleContext', () => ({
    useModuleContext: vi.fn(),
}));

vi.mock('@app/homeV3/modules/domains/useDomainModuleUtils', () => ({
    default: () => ({ renderDomainCounts: vi.fn(), navigateToDomains: vi.fn() }),
}));

vi.mock('@app/homeV3/module/components/LargeModule', () => ({
    default: ({ children }: React.PropsWithChildren<object>) => <div>{children}</div>,
}));

vi.mock('@app/homeV3/module/components/EmptyContent', () => ({
    default: () => <div data-testid="empty-content" />,
}));

vi.mock('@app/homeV3/module/components/EntityItem', () => ({
    default: () => <div data-testid="domain-item" />,
}));

vi.mock('@phosphor-icons/react/dist/csr/Globe', () => ({ Globe: () => <svg /> }));

vi.mock('react-i18next', () => ({
    useTranslation: () => ({ t: (key: string) => key }),
}));

describe('TopDomainsModule reload behavior', () => {
    const refetch = vi.fn<() => Promise<unknown>>();
    const onReloadingFinished = vi.fn();
    let isReloading = true;

    beforeEach(() => {
        vi.clearAllMocks();
        isReloading = true;
        vi.mocked(useGetDomains).mockReturnValue({
            domains: [],
            loading: false,
            refetch,
        });
        vi.mocked(useModuleContext).mockImplementation(
            () =>
                ({
                    isReloading,
                    onReloadingFinished,
                }) as ReturnType<typeof useModuleContext>,
        );
    });

    it('refetches when mounted with a pending reload', async () => {
        refetch.mockResolvedValue(undefined);
        render(<TopDomainsModule {...({} as React.ComponentProps<typeof TopDomainsModule>)} />);

        expect(refetch).toHaveBeenCalledOnce();
        await waitFor(() => expect(onReloadingFinished).toHaveBeenCalledOnce());
    });

    it('refetches after an explicit reload transition', async () => {
        refetch.mockResolvedValue(undefined);
        const { rerender } = render(<TopDomainsModule {...({} as React.ComponentProps<typeof TopDomainsModule>)} />);

        await waitFor(() => expect(refetch).toHaveBeenCalledOnce());

        isReloading = false;
        rerender(<TopDomainsModule {...({} as React.ComponentProps<typeof TopDomainsModule>)} />);
        isReloading = true;
        rerender(<TopDomainsModule {...({} as React.ComponentProps<typeof TopDomainsModule>)} />);

        expect(refetch).toHaveBeenCalledTimes(2);
        await waitFor(() => expect(onReloadingFinished).toHaveBeenCalledTimes(2));
    });

    it('finishes a failed reload so a later reload can be requested', async () => {
        refetch.mockRejectedValue(new Error('refresh failed'));
        const { rerender } = render(<TopDomainsModule {...({} as React.ComponentProps<typeof TopDomainsModule>)} />);

        await waitFor(() => expect(onReloadingFinished).toHaveBeenCalledOnce());

        isReloading = false;
        rerender(<TopDomainsModule {...({} as React.ComponentProps<typeof TopDomainsModule>)} />);
        isReloading = true;
        rerender(<TopDomainsModule {...({} as React.ComponentProps<typeof TopDomainsModule>)} />);

        await waitFor(() => expect(onReloadingFinished).toHaveBeenCalledTimes(2));
    });
});
