import { render, screen } from '@testing-library/react';
import React from 'react';
import { ThemeProvider } from 'styled-components';
import { describe, expect, it, vi } from 'vitest';

import { type MentionEntities, MentionEntitiesContext } from '@app/sharedV2/editor/mentions/MentionEntitiesContext';
import { MentionView } from '@app/sharedV2/editor/mentions/MentionView';
import themeV2 from '@conf/theme/themeV2';
import { useGetEntityMentionChipQuery, useGetEntityMentionNodeQuery } from '@src/graphql/search.generated';

vi.mock('@src/graphql/search.generated', () => ({
    useGetEntityMentionChipQuery: vi.fn(),
    useGetEntityMentionNodeQuery: vi.fn(),
}));

vi.mock('@src/app/useEntityRegistry', () => ({
    useEntityRegistry: () => ({ getDisplayName: (_type: string, entity: { name: string }) => entity.name }),
}));

vi.mock('@app/searchV2/autoCompleteV2/components/icon/EntityIcon', () => ({
    default: () => <span data-testid="entity-icon" />,
}));

// Renders the card next to the chip, as if the tooltip were open.
vi.mock('@src/app/recommendations/renderer/component/HoverEntityTooltip', () => ({
    HoverEntityTooltip: ({ children, card }: { children: React.ReactNode; card?: React.ReactNode }) => (
        <>
            {children}
            <div data-testid="hover-card">{card}</div>
        </>
    ),
}));

vi.mock('@app/sharedV2/hoverCard/EntityHoverCard', () => ({
    default: ({ entity }: { entity: { description?: string } }) => <span>{entity.description ?? 'no details'}</span>,
}));

function renderNode(
    result: { data?: unknown; loading: boolean },
    batch: MentionEntities | null = null,
    cardResult: { data?: unknown; loading: boolean } = { loading: true },
) {
    const mentions = batch && { ready: true, observe: () => () => {}, batch };
    vi.mocked(useGetEntityMentionChipQuery).mockReturnValue(
        result as unknown as ReturnType<typeof useGetEntityMentionChipQuery>,
    );
    vi.mocked(useGetEntityMentionNodeQuery).mockReturnValue(
        cardResult as unknown as ReturnType<typeof useGetEntityMentionNodeQuery>,
    );
    return render(
        <ThemeProvider theme={themeV2}>
            <MentionEntitiesContext.Provider value={mentions}>
                <MentionView urn="urn:li:corpuser:alice" name="Alice" />
            </MentionEntitiesContext.Provider>
        </ThemeProvider>,
    );
}

function batchOf(entities: Record<string, unknown>, loading = false): MentionEntities {
    return {
        urns: new Set(['urn:li:corpuser:alice']),
        loading,
        entitiesByUrn: new Map(Object.entries(entities)) as MentionEntities['entitiesByUrn'],
    };
}

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

    it('renders the entity from the batch without its own request', () => {
        renderNode(
            { loading: true },
            batchOf({
                'urn:li:corpuser:alice': { urn: 'urn:li:corpuser:alice', type: 'CORP_USER', name: 'Alice Smith' },
            }),
        );

        expect(screen.getByText('Alice Smith')).toBeInTheDocument();
        expect(vi.mocked(useGetEntityMentionChipQuery).mock.lastCall?.[0]?.skip).toBe(true);
    });

    it('shows the stored name while the batch loads', () => {
        renderNode({ loading: false }, batchOf({}, true));

        expect(screen.getByText('Alice')).toBeInTheDocument();
        expect(screen.queryByTestId('entity-icon')).not.toBeInTheDocument();
    });

    it('fills the hover card with the full entity, showing the chip data until it loads', () => {
        const chip = { urn: 'urn:li:corpuser:alice', type: 'CORP_USER', name: 'Alice Smith' };
        const { unmount } = renderNode({ loading: false, data: { entity: chip } });
        expect(screen.getByTestId('hover-card')).toHaveTextContent('no details');
        unmount();

        renderNode({ loading: false, data: { entity: chip } }, null, {
            loading: false,
            data: { entity: { ...chip, description: 'Owns the sales data' } },
        });
        expect(screen.getByTestId('hover-card')).toHaveTextContent('Owns the sales data');
    });
});
