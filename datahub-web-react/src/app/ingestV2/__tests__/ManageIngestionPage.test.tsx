import { MockedProvider } from '@apollo/client/testing';
import { render, screen, waitFor } from '@testing-library/react';
import React from 'react';
import { Route } from 'react-router';
import { describe, expect, it, vi } from 'vitest';

import { ManageIngestionPage } from '@app/ingestV2/ManageIngestionPage';
import { DEFAULT_APP_CONFIG } from '@src/appConfigContext';
import TestPageContainer from '@utils/test-utils/TestPageContainer';

const useUserContextMock = vi.fn();
const useAppConfigMock = vi.fn();
vi.mock('@app/context/useUserContext', () => ({
    useUserContext: () => useUserContextMock(),
}));
vi.mock('@app/useAppConfig', () => ({
    useAppConfig: () => useAppConfigMock(),
}));
vi.mock('@app/ingestV2/source/IngestionSourceList', () => ({
    IngestionSourceList: () => <div data-testid="sources-list" />,
}));
vi.mock('@app/ingestV2/executions/ExecutionsTab', () => ({
    ExecutionsTab: () => <div data-testid="executions-tab" />,
}));
vi.mock('@app/ingestV2/secret/SecretsList', () => ({
    SecretsList: () => <div data-testid="secrets-list" />,
}));
vi.mock('@app/onboarding/OnboardingTour', () => ({
    OnboardingTour: () => null,
}));

function setup({
    manageIngestion,
    viewIngestionSourcePrivilegesEnabled,
    initialPath,
}: {
    manageIngestion: boolean;
    viewIngestionSourcePrivilegesEnabled: boolean;
    initialPath: string;
}) {
    useUserContextMock.mockReturnValue({
        loaded: true,
        platformPrivileges: { manageIngestion, manageSecrets: false },
    });
    useAppConfigMock.mockReturnValue({
        loaded: true,
        config: {
            ...DEFAULT_APP_CONFIG,
            managedIngestionConfig: { enabled: true },
            featureFlags: {
                ...DEFAULT_APP_CONFIG.featureFlags,
                viewIngestionSourcePrivilegesEnabled,
            },
        },
    });

    render(
        <MockedProvider mocks={[]}>
            <TestPageContainer initialEntries={[initialPath]}>
                <ManageIngestionPage />
                <Route render={({ location }) => <div data-testid="current-path">{location.pathname}</div>} />
            </TestPageContainer>
        </MockedProvider>,
    );
}

describe('ManageIngestionPage', () => {
    it('shows the Sources tab without Run History to a viewer when the flag is on', async () => {
        setup({ manageIngestion: false, viewIngestionSourcePrivilegesEnabled: true, initialPath: '/ingestion' });

        expect(await screen.findByRole('tab', { name: 'Sources' })).toBeInTheDocument();
        expect(screen.queryByRole('tab', { name: 'Run History' })).not.toBeInTheDocument();
        expect(screen.getByTestId('create-ingestion-source-button')).toBeDisabled();
        expect(screen.getByTestId('current-path')).toHaveTextContent('/ingestion/sources');
    });

    it('redirects a viewer from the run history URL to Sources', async () => {
        setup({
            manageIngestion: false,
            viewIngestionSourcePrivilegesEnabled: true,
            initialPath: '/ingestion/run-history',
        });

        await waitFor(() => expect(screen.getByTestId('current-path')).toHaveTextContent('/ingestion/sources'));
        expect(screen.getByRole('tab', { name: 'Sources' })).toHaveAttribute('aria-selected', 'true');
        expect(screen.queryByTestId('executions-tab')).not.toBeInTheDocument();
    });

    it('shows no ingestion page to a viewer when the flag is off', async () => {
        setup({ manageIngestion: false, viewIngestionSourcePrivilegesEnabled: false, initialPath: '/ingestion' });

        await waitFor(() => expect(screen.queryByTestId('current-path')).toBeInTheDocument());
        expect(screen.queryByRole('tab', { name: 'Sources' })).not.toBeInTheDocument();
        expect(screen.queryByTestId('sources-list')).not.toBeInTheDocument();
    });

    it('keeps the Run History tab for a manager', async () => {
        setup({ manageIngestion: true, viewIngestionSourcePrivilegesEnabled: false, initialPath: '/ingestion' });

        expect(await screen.findByRole('tab', { name: 'Run History' })).toBeInTheDocument();
        expect(screen.getByTestId('create-ingestion-source-button')).toBeEnabled();
    });
});
