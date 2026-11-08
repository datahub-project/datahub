import { MockedProvider } from '@apollo/client/testing';
import { render, waitFor } from '@testing-library/react';
import React from 'react';

import { Routes } from '@app/Routes';
import { EmbeddedProfile, SidebarEntityHeader } from '@app/entityV2/shared/profileChunks';
import { mocks } from '@src/Mocks';
import TestPageContainer from '@utils/test-utils/TestPageContainer';

test('renders embed page properly', async () => {
    // The dataset name is rendered inside these React.lazy chunks. Load them before render
    // so findByText only has to wait for the GraphQL mock to resolve.
    await Promise.all([EmbeddedProfile.preload(), SidebarEntityHeader.preload()]);

    const { findByText } = render(
        <MockedProvider mocks={mocks} addTypename={false}>
            <TestPageContainer initialEntries={['/embed/dataset/urn:li:dataset:3']}>
                <Routes />
            </TestPageContainer>
        </MockedProvider>,
    );

    // AuthenticatedApp and EmbedRoutes are route-level React.lazy boundaries, so the profile
    // renders after findByText's 1s default on a loaded CI runner.
    expect(await findByText('Yet Another Dataset', {}, { timeout: 20_000 })).toBeInTheDocument();
}, 30_000);

test('shows 404 for missing mfe route when some mfes are active', async () => {
    // Mock the /mfe/config endpoint to return a configuration with one valid MFE
    global.fetch = vi.fn().mockImplementation((url) => {
        if (url === '/mfe/config') {
            return Promise.resolve({
                ok: true,
                text: () =>
                    Promise.resolve(`subNavigationMode: false
microFrontends:
  - id: myapp
    label: myapp from Yaml
    path: /myapp-mfe
    remoteEntry: http://localhost:9111/remoteEntry.js
    module: myapp/mount
    flags:
      enabled: true
      showInNav: false
    navIcon: Globe`),
            });
        }
        return Promise.reject(new Error(`Unhandled fetch: ${url}`));
    });

    const { getByText } = render(
        <MockedProvider mocks={mocks} addTypename={false}>
            <TestPageContainer initialEntries={['/mfe/missing']}>
                <Routes />
            </TestPageContainer>
        </MockedProvider>,
    );

    // Reaching the 404 resolves a chain of route-level React.lazy boundaries
    // (Routes -> AuthenticatedApp -> ProtectedRoutes -> SearchRoutes -> MFERoutes), so the
    // Suspense fallback outlives waitFor's 1s default on a loaded CI runner.
    await waitFor(() => expect(getByText(/not found/)).toBeInTheDocument(), { timeout: 30_000 });

    vi.restoreAllMocks();
});
