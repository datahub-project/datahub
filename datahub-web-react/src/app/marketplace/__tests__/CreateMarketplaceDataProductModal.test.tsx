import { MockedProvider } from '@apollo/client/testing';
import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import React from 'react';

import CreateMarketplaceDataProductModal from '@app/marketplace/CreateMarketplaceDataProductModal';
import TestPageContainer from '@utils/test-utils/TestPageContainer';

import { CreateDataProductDocument } from '@graphql/dataProduct.generated';

vi.mock('@app/entityV2/shared/DomainSelector/DomainSelector', () => ({
    default: () => <div data-testid="domain-selector" />,
}));

vi.mock('@app/entityV2/domain/DataProductsTab/DataProductBuilderForm', () => ({
    default: ({
        builderState,
        updateBuilderState,
    }: {
        builderState: { name: string };
        updateBuilderState: (state: { name: string }) => void;
    }) => (
        <input
            data-testid="name-input"
            value={builderState.name}
            onChange={(e) => updateBuilderState({ ...builderState, name: e.target.value })}
        />
    ),
}));

describe('CreateMarketplaceDataProductModal', () => {
    it('allows creating a data product without a domain', async () => {
        const mutationResult = vi.fn(() => ({ data: { createDataProduct: null } }));
        const mocks = [
            {
                request: { query: CreateDataProductDocument },
                variableMatcher: (variables: { input: Record<string, unknown> }) =>
                    !('domainUrn' in variables.input) ||
                    (variables.input.domainUrn === undefined && variables.input.properties !== undefined),
                result: mutationResult,
            },
        ];

        render(
            <MockedProvider mocks={mocks}>
                <TestPageContainer>
                    <CreateMarketplaceDataProductModal open onClose={vi.fn()} onCreateDataProduct={vi.fn()} />
                </TestPageContainer>
            </MockedProvider>,
        );

        const submit = screen.getByTestId('submit-button');
        expect(submit).toBeDisabled();

        fireEvent.change(screen.getByTestId('name-input'), { target: { value: 'My data product' } });
        expect(submit).toBeEnabled();

        fireEvent.click(submit);
        await waitFor(() => expect(mutationResult).toHaveBeenCalled());
    });
});
