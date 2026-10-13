import { render, screen } from '@testing-library/react';
import React from 'react';
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

function makeField(fieldPath: string, nullable: boolean, nativeDataType = 'string', description?: string) {
    return { fieldPath, nullable, nativeDataType, description, type: SchemaFieldDataType.String };
}

function renderWithSignature(signature: unknown) {
    mockUseBaseEntity.mockReturnValue({ entity: { __typename: 'Api', signature } });
    return render(
        <CustomThemeProvider>
            <SignatureTab />
        </CustomThemeProvider>,
    );
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

    it('handles a missing signature without crashing', () => {
        renderWithSignature(undefined);

        expect(screen.getByText('api.signature.noInput')).toBeInTheDocument();
        expect(screen.getByText('api.signature.noOutput')).toBeInTheDocument();
    });
});
