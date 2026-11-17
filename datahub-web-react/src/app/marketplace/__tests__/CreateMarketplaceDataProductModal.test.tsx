import { MockedProvider } from '@apollo/client/testing';
import { toast } from '@components';
import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import React from 'react';

import CreateMarketplaceDataProductModal from '@app/marketplace/CreateMarketplaceDataProductModal';
import TestPageContainer from '@utils/test-utils/TestPageContainer';

import { CreateDataProductDocument } from '@graphql/dataProduct.generated';
import { EntityType } from '@types';

vi.mock('@app/entityV2/shared/DomainSelector/DomainSelector', () => ({
    default: ({ onDomainsChange }: { onDomainsChange: (urns: string[]) => void }) => (
        <button
            type="button"
            data-testid="domain-selector"
            onClick={() => onDomainsChange(['urn:li:domain:marketing'])}
        >
            select domain
        </button>
    ),
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
                variableMatcher: (variables: { input: { domainUrn?: string; properties?: { name?: string } } }) =>
                    variables.input.domainUrn === undefined && variables.input.properties?.name === 'My data product',
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

    it('sends the selected domain urn when a domain is chosen', async () => {
        const mutationResult = vi.fn(() => ({ data: { createDataProduct: null } }));
        const mocks = [
            {
                request: { query: CreateDataProductDocument },
                variableMatcher: (variables: { input: { domainUrn?: string; properties?: { name?: string } } }) =>
                    variables.input.domainUrn === 'urn:li:domain:marketing' &&
                    variables.input.properties?.name === 'My data product',
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

        fireEvent.click(screen.getByTestId('domain-selector'));
        fireEvent.change(screen.getByTestId('name-input'), { target: { value: 'My data product' } });
        fireEvent.click(screen.getByTestId('submit-button'));
        await waitFor(() => expect(mutationResult).toHaveBeenCalled());
    });
    it('passes the created data product to the caller and closes on success', async () => {
        const created = { urn: 'urn:li:dataProduct:new', type: EntityType.DataProduct };
        const mocks = [
            {
                request: { query: CreateDataProductDocument },
                variableMatcher: () => true,
                result: { data: { createDataProduct: created } },
            },
        ];
        const onClose = vi.fn();
        const onCreateDataProduct = vi.fn();

        render(
            <MockedProvider mocks={mocks} addTypename={false}>
                <TestPageContainer>
                    <CreateMarketplaceDataProductModal
                        open
                        onClose={onClose}
                        onCreateDataProduct={onCreateDataProduct}
                    />
                </TestPageContainer>
            </MockedProvider>,
        );

        fireEvent.change(screen.getByTestId('name-input'), { target: { value: 'My data product' } });
        fireEvent.click(screen.getByTestId('submit-button'));

        await waitFor(() => expect(onCreateDataProduct).toHaveBeenCalledWith(expect.objectContaining(created)));
        expect(onClose).toHaveBeenCalled();
    });

    it('stays open when the mutation fails', async () => {
        const mocks = [
            {
                request: { query: CreateDataProductDocument },
                variableMatcher: () => true,
                error: new Error('Unauthorized'),
            },
        ];
        const toastError = vi.spyOn(toast, 'error');
        const onClose = vi.fn();
        const onCreateDataProduct = vi.fn();

        render(
            <MockedProvider mocks={mocks}>
                <TestPageContainer>
                    <CreateMarketplaceDataProductModal
                        open
                        onClose={onClose}
                        onCreateDataProduct={onCreateDataProduct}
                    />
                </TestPageContainer>
            </MockedProvider>,
        );

        fireEvent.change(screen.getByTestId('name-input'), { target: { value: 'My data product' } });
        fireEvent.click(screen.getByTestId('submit-button'));

        await waitFor(() => expect(toastError).toHaveBeenCalled());
        expect(onCreateDataProduct).not.toHaveBeenCalled();
        expect(onClose).not.toHaveBeenCalled();
    });
});
