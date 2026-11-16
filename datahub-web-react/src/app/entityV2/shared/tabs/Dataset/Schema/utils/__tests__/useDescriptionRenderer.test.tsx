import { MockedProvider } from '@apollo/client/testing';
import { act } from '@testing-library/react';
import React from 'react';
import { describe, expect, it, vi } from 'vitest';

import useDescriptionRenderer from '@app/entityV2/shared/tabs/Dataset/Schema/utils/useDescriptionRenderer';
import { renderHook } from '@utils/test-utils/renderHook';

import { SchemaField } from '@types';

vi.mock('@app/entityV2/shared/EntityContext', () => ({
    useMutationUrn: () => 'urn:li:dataset:test',
    useRefetch: () => undefined,
}));

const field = (fieldPath: string) => ({ fieldPath, description: `About ${fieldPath}` }) as SchemaField;
const ID = field('id');
const NAME = field('name');

const wrapper = ({ children }: { children: React.ReactNode }) => (
    <MockedProvider mocks={[]} addTypename={false}>
        {children}
    </MockedProvider>
);

describe('useDescriptionRenderer', () => {
    // Rows shift position when a struct expands or the table is re-sorted, so the expanded state has to
    // follow the field rather than the row index.
    it('keeps a description expanded for its field only', () => {
        const { result } = renderHook(() => useDescriptionRenderer(undefined, false), { wrapper });

        act(() => result.current(ID.description ?? undefined, ID).props.onExpanded(true));

        expect(result.current(ID.description ?? undefined, ID).props.expanded).toBe(true);
        expect(result.current(NAME.description ?? undefined, NAME).props.expanded).toBe(false);
    });
});
