import { MockedProvider } from '@apollo/client/testing';
import { render, screen } from '@testing-library/react';
import React from 'react';
import { ThemeProvider } from 'styled-components';

import { ManageEnvironments } from '@app/settingsV2/environments/ManageEnvironments';
import themes from '@conf/theme/themes';

import { EntityType } from '@types';

const settings = { enabled: true, canManage: true };

vi.mock('@app/context/useUserContext', () => ({
    useUserContext: () => ({ platformPrivileges: { manageCustomFabricTypes: settings.canManage } }),
}));

vi.mock('@app/useAppConfig', () => ({
    useAppConfig: () => ({ config: { featureFlags: { customFabricTypesEnabled: settings.enabled } } }),
}));

vi.mock('@graphql/customFabricType.generated', () => ({
    useListCustomFabricTypesQuery: () => ({
        loading: false,
        refetch: vi.fn(),
        data: {
            listCustomFabricTypes: {
                start: 0,
                count: 2,
                total: 2,
                customFabricTypes: [
                    {
                        urn: 'urn:li:customFabricType:PERF',
                        type: EntityType.CustomFabricType,
                        id: 'PERF',
                        properties: { displayName: 'Performance', description: 'Load tests' },
                        deprecation: null,
                    },
                    {
                        urn: 'urn:li:customFabricType:OLD',
                        type: EntityType.CustomFabricType,
                        id: 'OLD',
                        properties: null,
                        deprecation: { deprecated: true, note: 'Retired', actor: 'urn:li:corpuser:admin' },
                    },
                ],
            },
        },
    }),
    useCreateCustomFabricTypeMutation: () => [vi.fn()],
    useUpdateCustomFabricTypeMutation: () => [vi.fn()],
    useDeleteCustomFabricTypeMutation: () => [vi.fn()],
}));

const renderPage = () =>
    render(
        <MockedProvider mocks={[]} addTypename={false}>
            <ThemeProvider theme={themes.themeV2}>
                <ManageEnvironments />
            </ThemeProvider>
        </MockedProvider>,
    );

describe('ManageEnvironments', () => {
    beforeEach(() => {
        settings.enabled = true;
        settings.canManage = true;
    });

    it('explains that custom environments are off, with nothing to manage', () => {
        settings.enabled = false;
        renderPage();

        expect(screen.getByText('Custom environments are turned off')).toBeInTheDocument();
        expect(screen.queryByTestId('create-environment-button')).not.toBeInTheDocument();
        expect(screen.queryByText('Performance')).not.toBeInTheDocument();
    });

    it('lists environments by display name, marks deprecated ones, and lets a manager act on them', () => {
        renderPage();

        expect(screen.getByText('Performance')).toBeInTheDocument();
        expect(screen.getByText('PERF')).toBeInTheDocument();
        expect(screen.getByText('OLD')).toBeInTheDocument();
        expect(screen.getAllByText('Deprecated')).toHaveLength(1);
        expect(screen.getByTestId('create-environment-button')).toBeInTheDocument();
        expect(screen.getByTestId('environment-actions-PERF')).toBeInTheDocument();
    });

    it('shows the list read-only without the manage privilege', () => {
        settings.canManage = false;
        renderPage();

        expect(screen.getByText('Performance')).toBeInTheDocument();
        expect(screen.queryByTestId('create-environment-button')).not.toBeInTheDocument();
        expect(screen.queryByTestId('environment-actions-PERF')).not.toBeInTheDocument();
    });
});
