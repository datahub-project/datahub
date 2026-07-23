import { MockedProvider } from '@apollo/client/testing';
import { render, screen } from '@testing-library/react';
import React from 'react';
import { describe, expect, it, vi } from 'vitest';

import CreateLogicalModelButton from '@app/entityV2/shared/logicalModels/CreateLogicalModelButton';
import TestPageContainer from '@utils/test-utils/TestPageContainer';

const useUserContextMock = vi.fn();
const useAppConfigMock = vi.fn();
vi.mock('@app/context/useUserContext', () => ({
    useUserContext: () => useUserContextMock(),
}));
vi.mock('@app/useAppConfig', () => ({
    useAppConfig: () => useAppConfigMock(),
}));

function renderButton() {
    render(
        <MockedProvider mocks={[]}>
            <TestPageContainer>
                <CreateLogicalModelButton />
            </TestPageContainer>
        </MockedProvider>,
    );
}

describe('CreateLogicalModelButton', () => {
    it('renders when flag + privilege are on', () => {
        useUserContextMock.mockReturnValue({ platformPrivileges: { createLogicalModels: true } });
        useAppConfigMock.mockReturnValue({ config: { featureFlags: { logicalModelsEnabled: true } } });

        renderButton();

        expect(screen.getByTestId('create-logical-model-trigger')).toBeInTheDocument();
    });

    it('does not render when the feature flag is off', () => {
        useUserContextMock.mockReturnValue({ platformPrivileges: { createLogicalModels: true } });
        useAppConfigMock.mockReturnValue({ config: { featureFlags: { logicalModelsEnabled: false } } });

        renderButton();

        expect(screen.queryByTestId('create-logical-model-trigger')).not.toBeInTheDocument();
    });

    it('does not render when the user lacks the create privilege', () => {
        useUserContextMock.mockReturnValue({ platformPrivileges: { createLogicalModels: false } });
        useAppConfigMock.mockReturnValue({ config: { featureFlags: { logicalModelsEnabled: true } } });

        renderButton();

        expect(screen.queryByTestId('create-logical-model-trigger')).not.toBeInTheDocument();
    });
});
