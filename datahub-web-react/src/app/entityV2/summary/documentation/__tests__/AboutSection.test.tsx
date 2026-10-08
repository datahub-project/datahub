import { render, screen, waitFor } from '@testing-library/react';
import React, { createElement, forwardRef, useState } from 'react';
import { MemoryRouter } from 'react-router-dom';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import type { Mock } from 'vitest';

import { useEntityData } from '@app/entity/shared/EntityContext';
import AboutSection from '@app/entityV2/summary/documentation/AboutSection';
import CustomThemeProvider from '@src/CustomThemeProvider';

import { EntityType } from '@types';

const URN = 'urn:li:glossaryTerm:test';
const DESCRIPTION = 'The documentation that was already saved';

// setupTests.ts stands the editor in with a component that re-renders whenever `content` changes.
// The real EditorImpl hands `content` to useRemirror only at mount, so it cannot pick up a
// description that arrives after it mounted. Model that mount-only behaviour here, otherwise the
// stand-in papers over the very bug this test is about.
vi.mock('@components/components/Editor/Editor', () => ({
    Editor: forwardRef(
        ({ content, className, dataTestId }: { content?: string; className?: string; dataTestId?: string }, ref) => {
            const [contentAtMount] = useState(content ?? '');
            return createElement('div', { ref, className, 'data-testid': dataTestId }, contentAtMount);
        },
    ),
}));

vi.mock('@app/entity/shared/EntityContext', () => ({
    useEntityData: vi.fn(),
    useMutationUrn: () => URN,
    useRefetch: () => vi.fn(),
    useEntityUpdate: () => undefined,
}));

vi.mock('@app/entityV2/summary/documentation/useDocumentationPermission', () => ({
    useDocumentationPermission: () => true,
}));

vi.mock('@app/entityV2/summary/links/RelatedSection', () => ({
    default: () => null,
}));

vi.mock('@app/useEntityRegistry', () => ({
    useEntityRegistryV2: () => ({ getEntityName: () => 'Glossary Term' }),
}));

vi.mock('@graphql/mutations.generated', () => ({
    useUpdateDescriptionMutation: () => [vi.fn()],
}));

vi.mock('@app/shared/hooks/useFileUpload', () => ({
    default: () => ({ uploadFile: vi.fn() }),
}));

vi.mock('@app/shared/hooks/useFileUploadAnalyticsCallbacks', () => ({
    default: () => ({}),
}));

const setEntityData = (entityData: Record<string, unknown> | undefined) =>
    (useEntityData as Mock).mockReturnValue({ entityData, urn: URN, entityType: EntityType.GlossaryTerm });

const tree = () => (
    <MemoryRouter initialEntries={[`/glossaryTerm/${URN}/Summary?editingDescription=true`]}>
        <CustomThemeProvider>
            <AboutSection hideLinksButton />
        </CustomThemeProvider>
    </MemoryRouter>
);

describe('AboutSection', () => {
    beforeEach(() => {
        vi.clearAllMocks();
    });

    // A cold load of ?editingDescription=true opens the modal before entityData resolves. If the
    // editor keeps the empty description it mounted with, pressing Publish overwrites the saved
    // documentation with an empty string.
    it('should show the description in the edit modal when it resolves after the modal opened', async () => {
        setEntityData(undefined);
        const { rerender } = render(tree());

        expect(screen.getByTestId('description-editor')).toBeEmptyDOMElement();

        setEntityData({ editableProperties: { description: DESCRIPTION } });
        rerender(tree());

        await waitFor(() => expect(screen.getByTestId('description-editor')).toHaveTextContent(DESCRIPTION));
    });
});
