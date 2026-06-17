import { render, screen } from '@testing-library/react';
import React from 'react';
import { ThemeProvider } from 'styled-components';

import { TagNameColumn } from '@app/tags/TagsTableColumns';
import themeV2 from '@conf/theme/themeV2';

import { useGetTagQuery } from '@src/graphql/tag.generated';

vi.mock('@src/graphql/tag.generated', () => ({
    useGetTagQuery: vi.fn(() => ({ data: null })),
}));

vi.mock('@app/entityV2/shared/components/styled/DeprecationIcon', () => ({
    DeprecationIcon: () => <div data-testid="deprecation-icon" />,
}));

vi.mock('react-highlighter', () => ({
    default: ({ children }: { children: React.ReactNode }) => <>{children}</>,
}));

const TAG_URN = 'urn:li:tag:TestTag';

const renderColumn = (mockData: any) => {
    vi.mocked(useGetTagQuery).mockReturnValue(mockData);
    return render(
        <ThemeProvider theme={themeV2}>
            <TagNameColumn tagUrn={TAG_URN} displayName="TestTag" />
        </ThemeProvider>,
    );
};

describe('TagNameColumn', () => {
    afterEach(() => vi.clearAllMocks());

    it('renders deprecation icon when tag is deprecated', () => {
        renderColumn({ data: { tag: { deprecation: { deprecated: true } } } });
        expect(screen.getByTestId('deprecation-icon')).toBeInTheDocument();
    });

    it('does not render deprecation icon when deprecated=false', () => {
        renderColumn({ data: { tag: { deprecation: { deprecated: false } } } });
        expect(screen.queryByTestId('deprecation-icon')).not.toBeInTheDocument();
    });

    it('does not render deprecation icon when deprecation is null', () => {
        renderColumn({ data: { tag: { deprecation: null } } });
        expect(screen.queryByTestId('deprecation-icon')).not.toBeInTheDocument();
    });

    it('does not render deprecation icon when data is absent', () => {
        renderColumn({ data: null });
        expect(screen.queryByTestId('deprecation-icon')).not.toBeInTheDocument();
    });

    it('always renders the tag display name', () => {
        renderColumn({ data: null });
        expect(screen.getByText('TestTag')).toBeInTheDocument();
    });
});
