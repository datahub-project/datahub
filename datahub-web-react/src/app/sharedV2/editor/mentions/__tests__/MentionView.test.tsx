import { render, screen } from '@testing-library/react';
import React from 'react';
import { MemoryRouter } from 'react-router-dom';
import { ThemeProvider } from 'styled-components';
import { describe, expect, it, vi } from 'vitest';

import { MentionView } from '@app/sharedV2/editor/mentions/MentionView';
import themeV2 from '@conf/theme/themeV2';
import { useGetEntityMentionNodeQuery } from '@src/graphql/search.generated';

vi.mock('@src/graphql/search.generated', () => ({
    useGetEntityMentionNodeQuery: vi.fn(),
}));

vi.mock('@src/app/useEntityRegistry', () => ({
    useEntityRegistry: () => ({
        getDisplayName: (_type: string, entity: { name: string }) => entity.name,
        getEntityUrl: (_type: string, urn: string) => `/user/${urn}`,
    }),
}));

vi.mock('@app/searchV2/autoCompleteV2/components/icon/EntityIcon', () => ({
    default: () => <span data-testid="entity-icon" />,
}));

vi.mock('@src/app/recommendations/renderer/component/HoverEntityTooltip', () => ({
    HoverEntityTooltip: ({ children }: { children: React.ReactNode }) => <>{children}</>,
}));

function renderNode(result: { data?: unknown; loading: boolean }, readOnly = false) {
    vi.mocked(useGetEntityMentionNodeQuery).mockReturnValue(
        result as unknown as ReturnType<typeof useGetEntityMentionNodeQuery>,
    );
    return render(
        <MemoryRouter>
            <ThemeProvider theme={themeV2}>
                <MentionView urn="urn:li:corpuser:alice" name="Alice" readOnly={readOnly} />
            </ThemeProvider>
        </MemoryRouter>,
    );
}

const ALICE = { entity: { urn: 'urn:li:corpuser:alice', type: 'CORP_USER', name: 'Alice Smith' } };

describe('MentionView', () => {
    it('shows the stored name while the entity loads', () => {
        renderNode({ loading: true });

        expect(screen.getByText('Alice')).toBeInTheDocument();
        expect(screen.queryByTestId('entity-icon')).not.toBeInTheDocument();
    });

    it('shows the stored name without a chip when the entity does not exist', () => {
        renderNode({ loading: false, data: { entity: null } });

        expect(screen.getByText('Alice')).toBeInTheDocument();
        expect(screen.queryByTestId('entity-icon')).not.toBeInTheDocument();
    });

    it('renders a chip with the current display name and icon of the entity', () => {
        renderNode({
            loading: false,
            data: { entity: { urn: 'urn:li:corpuser:alice', type: 'CORP_USER', name: 'Alice Smith' } },
        });

        expect(screen.getByText('Alice Smith')).toBeInTheDocument();
        expect(screen.getByTestId('entity-icon')).toBeInTheDocument();
    });

    it('links a read-only chip to the entity page', () => {
        renderNode({ loading: false, data: ALICE }, true);

        expect(screen.getByRole('link')).toHaveAttribute('href', '/user/urn:li:corpuser:alice');
    });

    it('does not link a chip inside the editor', () => {
        renderNode({ loading: false, data: ALICE });

        expect(screen.queryByRole('link')).not.toBeInTheDocument();
    });
});
