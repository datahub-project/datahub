import { MockedProvider } from '@apollo/client/testing';
import { fireEvent, render, screen } from '@testing-library/react';
import { Form } from 'antd';
import React from 'react';
import { vi } from 'vitest';

import SchemaForm from '@app/ingestV2/source/builder/SchemaForm/SchemaForm';
import { MultiStepSourceBuilderState } from '@app/ingestV2/source/multiStepBuilder/types';
import TestPageContainer from '@utils/test-utils/TestPageContainer';

// The analytics layer runs at import time (checkAuthStatus) and reads `localStorage`.
// Stub it so the shared providers mount without a browser storage backend.
vi.mock('@app/analytics', () => ({
    __esModule: true,
    default: { identify: vi.fn(), event: vi.fn(), page: vi.fn() },
}));

// This test environment's jsdom does not expose a global `localStorage`; several shared
// providers (e.g. CustomThemeProvider) read it during render. Provide an in-memory stub.
if (typeof globalThis.localStorage === 'undefined') {
    const store = new Map<string, string>();
    vi.stubGlobal('localStorage', {
        getItem: (key: string) => store.get(key) ?? null,
        setItem: (key: string, value: string) => store.set(key, String(value)),
        removeItem: (key: string) => store.delete(key),
        clear: () => store.clear(),
    });
}

// Stub the secrets query so the SecretField (password) renders without a live backend.
vi.mock('@graphql/ingestion.generated', async (importOriginal) => {
    const actual = await importOriginal<Record<string, unknown>>();
    return {
        ...actual,
        useListSecretsQuery: () => ({
            data: { listSecrets: { start: 0, count: 0, total: 0, secrets: [] } },
            refetch: vi.fn(),
            loading: false,
            error: undefined,
        }),
    };
});

const DISPLAY_RECIPE = 'source:\n    type: redshift\n    config: {}\n';

function renderSchemaForm(setStagedRecipe: (recipe: string) => void) {
    const state = { type: 'redshift' } as MultiStepSourceBuilderState;

    function TestHost() {
        const [form] = Form.useForm();
        return (
            <SchemaForm
                state={state}
                displayRecipe={DISPLAY_RECIPE}
                form={form}
                runFormValidation={vi.fn()}
                setStagedRecipe={setStagedRecipe}
            />
        );
    }

    return render(
        <MockedProvider mocks={[]} addTypename={false}>
            <TestPageContainer>
                <TestHost />
            </TestPageContainer>
        </MockedProvider>,
    );
}

describe('SchemaForm', () => {
    it('renders the expanded Connection section with a Host Port field', () => {
        renderSchemaForm(vi.fn());

        expect(screen.getByText('Connection')).toBeInTheDocument();
        expect(screen.getByText('Host Port')).toBeInTheDocument();
    });

    it('renders a collapsed feature section header (Lineage)', () => {
        renderSchemaForm(vi.fn());

        expect(screen.getByText('Lineage')).toBeInTheDocument();
    });

    it('stages an updated recipe when a text field changes', () => {
        const setStagedRecipe = vi.fn();
        renderSchemaForm(setStagedRecipe);

        const hostPortInput = screen.getByPlaceholderText(/redshift\.company/);
        fireEvent.change(hostPortInput, { target: { value: 'my-cluster:5439' } });

        expect(setStagedRecipe).toHaveBeenCalled();
        const stagedRecipe = setStagedRecipe.mock.calls.at(-1)?.[0];
        expect(stagedRecipe).toContain('my-cluster:5439');
    });
});
