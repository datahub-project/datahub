import { render, screen } from '@testing-library/react';
import React from 'react';
import { MemoryRouter } from 'react-router-dom';
import { describe, expect, it, vi } from 'vitest';

import SignatureTab from '@app/entityV2/api/SignatureTab';
import CustomThemeProvider from '@src/CustomThemeProvider';

import { SchemaFieldDataType } from '@types';

// The tab reads the base entity from context; drive it directly per test.
const mockUseBaseEntity = vi.fn();
vi.mock('@app/entity/shared/EntityContext', () => ({
    useBaseEntity: () => mockUseBaseEntity(),
}));

// Identity translations keep assertions deterministic regardless of locale wiring.
vi.mock('react-i18next', () => ({
    useTranslation: () => ({ t: (key: string) => key }),
}));

vi.mock('@app/useEntityRegistry', () => ({
    useEntityRegistry: () => ({
        getEntityUrl: (_type: unknown, urn: string) => `/entity/${urn}`,
        getDisplayName: (_type: unknown, entity: { name?: string; urn: string }) => entity.name || entity.urn,
    }),
}));

vi.mock('@app/sharedV2/icons/PlatformIcon', () => ({ default: () => <span data-testid="platform-icon" /> }));

function makeField(fieldPath: string, nullable: boolean, nativeDataType = 'string', description?: string) {
    return { fieldPath, nullable, nativeDataType, description, type: SchemaFieldDataType.String };
}

function renderWithSignature(signature: unknown) {
    mockUseBaseEntity.mockReturnValue({ entity: { __typename: 'Api', signature } });
    return render(
        <MemoryRouter>
            <CustomThemeProvider>
                <SignatureTab />
            </CustomThemeProvider>
        </MemoryRouter>,
    );
}

const REQUEST_URN = 'urn:li:dataset:(urn:li:dataPlatform:grpc,orders.GetOrderRequest,PROD)';
const RESPONSE_URN = 'urn:li:dataset:(urn:li:dataPlatform:grpc,orders.Order,PROD)';

function makeDataset(urn: string, name: string) {
    return { urn, type: 'DATASET', name, platform: { name: 'grpc' }, subTypes: { typeNames: ['Message'] } };
}

describe('SignatureTab', () => {
    it('renders typed input parameters from the apiSignature aspect', () => {
        renderWithSignature({
            inputFields: [makeField('order_id', false, 'string', 'The order id.'), makeField('count', true, 'integer')],
            outputFields: [],
        });

        expect(screen.getByText('order_id')).toBeInTheDocument();
        expect(screen.getByText('count')).toBeInTheDocument();
        expect(screen.getByText('The order id.')).toBeInTheDocument();
        // A non-nullable field is required; a nullable one is optional.
        expect(screen.getByText('api.signature.valueRequired')).toBeInTheDocument();
        expect(screen.getByText('api.signature.valueOptional')).toBeInTheDocument();
    });

    it('renders a single scalar return via the Returns row', () => {
        renderWithSignature({ inputFields: [], outputFields: [makeField('result', true, 'string')] });

        expect(screen.getByText('api.signature.returns', { exact: false })).toBeInTheDocument();
    });

    it('shows empty states when the signature has no fields', () => {
        renderWithSignature({ inputFields: [], outputFields: [] });

        expect(screen.getByText('api.signature.noInput')).toBeInTheDocument();
        expect(screen.getByText('api.signature.noOutput')).toBeInTheDocument();
    });

    it('renders input/output datasets as links when the schema is defined by reference', () => {
        renderWithSignature({
            inputFields: [],
            outputFields: [],
            inputDatasets: [makeDataset(REQUEST_URN, 'orders.GetOrderRequest')],
            outputDatasets: [makeDataset(RESPONSE_URN, 'orders.Order')],
        });

        // By-reference sides use the schema titles, not the inline-parameter ones.
        expect(screen.getByText('api.signature.inputDatasetsTitle')).toBeInTheDocument();
        expect(screen.getByText('api.signature.outputDatasetsTitle')).toBeInTheDocument();
        expect(screen.queryByText('api.signature.noInput')).not.toBeInTheDocument();
        expect(screen.queryByText('api.signature.noOutput')).not.toBeInTheDocument();

        expect(screen.getByText('orders.GetOrderRequest').closest('a')).toHaveAttribute(
            'href',
            `/entity/${REQUEST_URN}`,
        );
        expect(screen.getByText('orders.Order').closest('a')).toHaveAttribute('href', `/entity/${RESPONSE_URN}`);
    });

    it('shows the dataset reference ahead of inline fields when both are present', () => {
        renderWithSignature({
            inputFields: [makeField('order_id', false)],
            outputFields: [],
            inputDatasets: [makeDataset(REQUEST_URN, 'orders.GetOrderRequest')],
        });

        expect(screen.getByText('orders.GetOrderRequest')).toBeInTheDocument();
        expect(screen.getByText('order_id')).toBeInTheDocument();
        expect(screen.getByText('api.signature.noOutput')).toBeInTheDocument();
    });

    it('handles a missing signature without crashing', () => {
        renderWithSignature(undefined);

        expect(screen.getByText('api.signature.noInput')).toBeInTheDocument();
        expect(screen.getByText('api.signature.noOutput')).toBeInTheDocument();
    });
});
