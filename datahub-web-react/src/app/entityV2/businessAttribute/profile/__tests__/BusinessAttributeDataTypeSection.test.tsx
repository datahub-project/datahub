import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import React from 'react';
import { beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';

import { BusinessAttributeDataTypeSection } from '@app/entityV2/businessAttribute/profile/BusinessAttributeDataTypeSection';
import CustomThemeProvider from '@src/CustomThemeProvider';

const { updateBusinessAttribute, refetch } = vi.hoisted(() => ({
    updateBusinessAttribute: vi.fn(),
    refetch: vi.fn(),
}));

const entityData = {
    properties: { businessAttributeDataType: 'STRING' },
};

vi.mock('@src/app/entity/shared/EntityContext', () => ({
    useEntityData: () => ({
        urn: 'urn:li:businessAttribute:revenue',
        entityType: 'BUSINESS_ATTRIBUTE',
        entityData,
    }),
    useRefetch: () => refetch,
}));

vi.mock('@graphql/businessAttribute.generated', () => ({
    useUpdateBusinessAttributeMutation: () => [updateBusinessAttribute],
}));

beforeAll(() => {
    vi.stubGlobal(
        'IntersectionObserver',
        class {
            private readonly callback: IntersectionObserverCallback;

            constructor(callback: IntersectionObserverCallback) {
                this.callback = callback;
            }

            observe(target: Element) {
                this.callback(
                    [{ isIntersecting: true, target } as IntersectionObserverEntry],
                    this as unknown as IntersectionObserver,
                );
            }

            unobserve() {}

            disconnect() {}

            takeRecords() {
                return [];
            }
        },
    );
});

const renderSection = () =>
    render(
        <CustomThemeProvider>
            <BusinessAttributeDataTypeSection />
        </CustomThemeProvider>,
    );

const chooseNumber = async () => {
    fireEvent.click(screen.getByTestId('edit-data-type-button'));
    fireEvent.click(await screen.findByTestId('add-data-type-option-base'));
    fireEvent.click(await screen.findByTestId('option-NUMBER'));
};

describe('BusinessAttributeDataTypeSection', () => {
    beforeEach(() => {
        updateBusinessAttribute.mockReset();
        refetch.mockReset();
    });

    it('keeps the chosen data type while the update is in flight', async () => {
        let resolveUpdate: (value: unknown) => void = () => {};
        updateBusinessAttribute.mockReturnValue(
            new Promise((resolve) => {
                resolveUpdate = resolve;
            }),
        );

        renderSection();
        await chooseNumber();

        expect(screen.getByTestId('add-data-type-option-base')).toHaveTextContent('NUMBER');
        expect(updateBusinessAttribute).toHaveBeenCalledWith({
            variables: { urn: 'urn:li:businessAttribute:revenue', input: { type: 'NUMBER' } },
        });

        resolveUpdate({});
        await waitFor(() => expect(refetch).toHaveBeenCalled());
    });

    it('restores the previous data type when the update fails', async () => {
        updateBusinessAttribute.mockRejectedValue(new Error('save failed'));

        renderSection();
        await chooseNumber();

        await waitFor(() => expect(screen.getByTestId('add-data-type-option-base')).toHaveTextContent('STRING'));
        expect(refetch).not.toHaveBeenCalled();
    });
});
