import { render } from '@testing-library/react';
import React from 'react';
import { MemoryRouter } from 'react-router-dom';

import ProductUpdates from '@app/shared/product/update/ProductUpdates';
import * as hooks from '@app/shared/product/update/hooks';
import * as useIsHomePageModule from '@app/shared/useIsHomePage';
import * as useAppConfigModule from '@app/useAppConfig';

const mockLatestUpdate = {
    id: 'test-update',
    enabled: true,
    title: "What's New",
    header: 'New Features',
    description: 'Check out our latest updates',
    primaryCtaText: 'Learn More',
    primaryCtaLink: '/updates',
    requiredVersion: '0.3.12',
    features: [],
};

const mockAppConfig = {
    config: {
        appVersion: '0.3.12',
        featureFlags: {},
        trialConfig: {
            trialEnabled: false,
        },
    },
    loaded: true,
};

describe('ProductUpdates', () => {
    beforeEach(() => {
        // Default mocks - component should show
        vi.spyOn(hooks, 'useIsProductAnnouncementEnabled').mockReturnValue(true);
        vi.spyOn(hooks, 'useGetLatestProductAnnouncementData').mockReturnValue(mockLatestUpdate);
        vi.spyOn(hooks, 'useIsProductAnnouncementVisible').mockReturnValue({
            visible: true,
            refetch: vi.fn(),
        });
        vi.spyOn(hooks, 'useDismissProductAnnouncement').mockReturnValue(vi.fn());
        vi.spyOn(useIsHomePageModule, 'useIsHomePage').mockReturnValue(true);
        vi.spyOn(useAppConfigModule, 'useAppConfig').mockReturnValue(mockAppConfig as any);
        vi.spyOn(useAppConfigModule, 'useIsFreeTrialInstance').mockReturnValue(false);
    });

    afterEach(() => {
        vi.restoreAllMocks();
    });

    it('renders when all conditions are met', () => {
        const { getByText } = render(
            <MemoryRouter>
                <ProductUpdates />
            </MemoryRouter>,
        );

        expect(getByText('New Features')).toBeInTheDocument();
    });

    it('does not render when feature is disabled', () => {
        vi.spyOn(hooks, 'useIsProductAnnouncementEnabled').mockReturnValue(false);

        const { container } = render(
            <MemoryRouter>
                <ProductUpdates />
            </MemoryRouter>,
        );

        expect(container.firstChild).toBeNull();
    });

    it('does not render when not visible', () => {
        vi.spyOn(hooks, 'useIsProductAnnouncementVisible').mockReturnValue({
            visible: false,
            refetch: vi.fn(),
        });

        const { container } = render(
            <MemoryRouter>
                <ProductUpdates />
            </MemoryRouter>,
        );

        expect(container.firstChild).toBeNull();
    });

    it('does not render when update is not enabled', () => {
        vi.spyOn(hooks, 'useGetLatestProductAnnouncementData').mockReturnValue({
            ...mockLatestUpdate,
            enabled: false,
        });

        const { container } = render(
            <MemoryRouter>
                <ProductUpdates />
            </MemoryRouter>,
        );

        expect(container.firstChild).toBeNull();
    });

    it('does not render when version does not match', () => {
        vi.spyOn(useAppConfigModule, 'useAppConfig').mockReturnValue({
            ...mockAppConfig,
            config: {
                ...mockAppConfig.config,
                appVersion: '0.3.11', // Different version
            },
        } as any);

        const { container } = render(
            <MemoryRouter>
                <ProductUpdates />
            </MemoryRouter>,
        );

        expect(container.firstChild).toBeNull();
    });

    it('does not render when not on home page', () => {
        vi.spyOn(useIsHomePageModule, 'useIsHomePage').mockReturnValue(false);

        const { container } = render(
            <MemoryRouter>
                <ProductUpdates />
            </MemoryRouter>,
        );

        expect(container.firstChild).toBeNull();
    });

    it('does not render on free trial instances', () => {
        vi.spyOn(useAppConfigModule, 'useIsFreeTrialInstance').mockReturnValue(true);

        const { container } = render(
            <MemoryRouter>
                <ProductUpdates />
            </MemoryRouter>,
        );

        expect(container.firstChild).toBeNull();
    });

    it('renders on non-free-trial instances when all other conditions are met', () => {
        vi.spyOn(useAppConfigModule, 'useIsFreeTrialInstance').mockReturnValue(false);

        const { getByText } = render(
            <MemoryRouter>
                <ProductUpdates />
            </MemoryRouter>,
        );

        expect(getByText('New Features')).toBeInTheDocument();
    });
});
