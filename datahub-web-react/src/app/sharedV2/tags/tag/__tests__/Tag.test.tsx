import { render, screen } from '@testing-library/react';
import React from 'react';
import { ThemeProvider } from 'styled-components';

import Tag from '@app/sharedV2/tags/tag/Tag';
import themeV2 from '@conf/theme/themeV2';

import { EntityType } from '@types';

vi.mock('@app/entityV2/shared/components/styled/DeprecationIcon', () => ({
    DeprecationIcon: () => <div data-testid="deprecation-icon" />,
}));

vi.mock('@app/recommendations/renderer/component/HoverEntityTooltip', () => ({
    HoverEntityTooltip: ({ children }: { children: React.ReactNode }) => <>{children}</>,
}));

vi.mock('@app/shared/tags/TagProfileDrawer', () => ({
    TagProfileDrawer: () => null,
}));

vi.mock('@app/sharedV2/modals/ConfirmationModal', () => ({
    ConfirmationModal: () => null,
}));

vi.mock('@app/useEntityRegistry', () => ({
    useEntityRegistry: () => ({ getDisplayName: vi.fn(() => 'TestTag') }),
}));

vi.mock('@app/search/context/SearchResultContext', () => ({
    useHasMatchedFieldByUrn: vi.fn(() => false),
}));

vi.mock('@app/shared/useEmbeddedProfileLinkProps', () => ({
    useIsEmbeddedProfile: vi.fn(() => false),
}));

vi.mock('@graphql/mutations.generated', () => ({
    useRemoveTagMutation: vi.fn(() => [vi.fn()]),
}));

vi.mock('@utils/runtimeBasePath', () => ({
    resolveRuntimePath: (p: string) => p,
}));

vi.mock('@components', async (importOriginal) => {
    const actual = await importOriginal<typeof import('@components')>();
    return { ...actual, toast: { success: vi.fn(), error: vi.fn() } };
});

const baseTag = {
    tag: {
        urn: 'urn:li:tag:TestTag',
        type: EntityType.Tag,
        name: 'TestTag',
        properties: { colorHex: null },
        deprecation: null,
    },
    associatedUrn: 'urn:li:dataset:test',
} as any;

const renderTag = (tag: any) =>
    render(
        <ThemeProvider theme={themeV2}>
            <Tag tag={tag} />
        </ThemeProvider>,
    );

describe('Tag (sharedV2)', () => {
    it('renders deprecation icon when tag is deprecated', () => {
        renderTag({
            ...baseTag,
            tag: { ...baseTag.tag, deprecation: { deprecated: true, note: null, actor: null, decommissionTime: null } },
        });
        expect(screen.getByTestId('deprecation-icon')).toBeInTheDocument();
    });

    it('does not render deprecation icon when deprecated=false', () => {
        renderTag({
            ...baseTag,
            tag: {
                ...baseTag.tag,
                deprecation: { deprecated: false, note: null, actor: null, decommissionTime: null },
            },
        });
        expect(screen.queryByTestId('deprecation-icon')).not.toBeInTheDocument();
    });

    it('does not render deprecation icon when deprecation is null', () => {
        renderTag(baseTag);
        expect(screen.queryByTestId('deprecation-icon')).not.toBeInTheDocument();
    });
});
