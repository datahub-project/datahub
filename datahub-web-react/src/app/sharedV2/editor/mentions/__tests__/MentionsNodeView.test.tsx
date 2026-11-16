import { render, screen } from '@testing-library/react';
import React from 'react';
import { ThemeProvider } from 'styled-components';
import { describe, expect, it, vi } from 'vitest';

import { MentionsNodeView } from '@app/sharedV2/editor/mentions/MentionsNodeView';
import themeV2 from '@conf/theme/themeV2';
import { useGetEntityMentionNodeQuery } from '@src/graphql/search.generated';

vi.mock('@src/graphql/search.generated', () => ({
    useGetEntityMentionNodeQuery: vi.fn(),
}));

vi.mock('@src/app/useEntityRegistry', () => ({
    useEntityRegistry: () => ({ getDisplayName: (_type: string, entity: { name: string }) => entity.name }),
}));

vi.mock('@app/searchV2/autoCompleteV2/components/icon/EntityIcon', () => ({
    default: () => <span data-testid="entity-icon" />,
}));

vi.mock('@src/app/recommendations/renderer/component/HoverEntityTooltip', () => ({
    HoverEntityTooltip: ({ children }: { children: React.ReactNode }) => <>{children}</>,
}));

function renderNode(result: { data?: unknown; loading: boolean }) {
    vi.mocked(useGetEntityMentionNodeQuery).mockReturnValue(
        result as unknown as ReturnType<typeof useGetEntityMentionNodeQuery>,
    );
    const node = { attrs: { urn: 'urn:li:corpuser:alice', name: 'Alice' } };
    return render(
        <ThemeProvider theme={themeV2}>
            <MentionsNodeView {...({ node } as unknown as React.ComponentProps<typeof MentionsNodeView>)} />
        </ThemeProvider>,
    );
}

describe('MentionsNodeView', () => {
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
});
