import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import React from 'react';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { EditColumn } from '@app/entity/shared/tabs/Properties/Edit/EditColumn';
import handleGraphQLError from '@app/shared/handleGraphQLError';
import { EntityType, StructuredPropertyEntity } from '@src/types.generated';

// Presentational pieces are stubbed: this test pins EditColumn's own wiring (menu item ->
// confirm -> mutation catch -> shared error handler), not the design system.
vi.mock('@src/alchemy-components', () => ({
    Button: (props: any) => (
        <button type="button" data-testid={props['data-testid']} onClick={props.onClick} />
    ),
    Menu: ({ items, children }: any) => (
        <div>
            {children}
            {items?.map((item: any) => (
                <button type="button" key={item.key} onClick={item.onClick}>
                    {item.title}
                </button>
            ))}
        </div>
    ),
}));

vi.mock('@src/app/sharedV2/modals/ConfirmationModal', () => ({
    ConfirmationModal: ({ isOpen, handleConfirm }: any) =>
        isOpen ? (
            <button type="button" data-testid="modal-confirm-button" onClick={handleConfirm} />
        ) : null,
}));

// Regression coverage for the remove flow: it must route failures through the shared
// handleGraphQLError (which surfaces validator rejection messages verbatim) instead of
// swallowing the error into a hardcoded toast. The wiring — not the handler internals —
// is what regressed, so the assertion is on the call into the handler.
const removeMock = vi.fn();

vi.mock('@src/graphql/structuredProperties.generated', () => ({
    useRemoveStructuredPropertiesMutation: () => [removeMock],
}));

vi.mock('@app/shared/handleGraphQLError', () => ({
    default: vi.fn(),
}));

vi.mock('@app/entity/shared/tabs/Properties/Edit/EditStructuredPropertyModal', () => ({
    default: () => null,
}));

vi.mock('@app/entity/shared/EntityContext', () => ({
    useEntityContext: () => ({ refetch: vi.fn() }),
    useEntityData: () => ({ entityType: EntityType.Dataset }),
    useMutationUrn: () => 'urn:li:dataset:(urn:li:dataPlatform:hive,test,PROD)',
}));

vi.mock('@src/app/analytics', () => ({
    default: { event: vi.fn() },
    EventType: { RemoveStructuredPropertyEvent: 'RemoveStructuredPropertyEvent' },
}));

vi.mock('@src/app/sharedV2/toastMessageUtils', () => ({
    ToastType: { LOADING: 'LOADING', SUCCESS: 'SUCCESS', ERROR: 'ERROR' },
    showToastMessage: vi.fn(),
}));

vi.mock('react-i18next', () => ({
    useTranslation: () => ({ t: (key: string) => key }),
}));

const structuredProperty = {
    urn: 'urn:li:structuredProperty:io.demo.testPriority',
    definition: {
        displayName: 'Test Priority',
        immutable: false,
        valueType: { urn: 'urn:li:dataType:datahub.string' },
    },
} as unknown as StructuredPropertyEntity;

describe('EditColumn remove flow error propagation', () => {
    beforeEach(() => {
        vi.clearAllMocks();
    });

    it('routes remove failures through handleGraphQLError with the default message', async () => {
        const validationError = {
            graphQLErrors: [
                {
                    message: 'structured property change blocked: live message file demo',
                    extensions: { code: 400, errorSource: 'VALIDATION' },
                },
            ],
        };
        removeMock.mockRejectedValueOnce(validationError);

        render(<EditColumn structuredProperty={structuredProperty} values={['P0']} />);

        // Open the "..." menu and click Remove, then confirm in the modal.
        fireEvent.click(screen.getByTestId('structured-prop-entity-more-icon'));
        fireEvent.click(await screen.findByText('common.actions:remove'));
        fireEvent.click(await screen.findByTestId('modal-confirm-button'));

        await waitFor(() => expect(handleGraphQLError).toHaveBeenCalledTimes(1));
        expect(handleGraphQLError).toHaveBeenCalledWith({
            error: validationError,
            defaultMessage: 'properties.removed.error',
        });
    });
});
