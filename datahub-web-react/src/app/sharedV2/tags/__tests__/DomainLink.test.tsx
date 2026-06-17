import { render, screen } from '@testing-library/react';
import React from 'react';
import { BrowserRouter } from 'react-router-dom';

import { DomainLink } from '@app/sharedV2/tags/DomainLink';
import themeV2 from '@conf/theme/themeV2';
import { ThemeProvider } from 'styled-components';

import { EntityType } from '@types';

vi.mock('@app/useEntityRegistry', () => ({
    useEntityRegistry: () => ({
        getDisplayName: vi.fn(() => 'Test Domain'),
        getEntityUrl: vi.fn(() => '/domain/test'),
    }),
}));

vi.mock('@app/shared/useEmbeddedProfileLinkProps', () => ({
    useEmbeddedProfileLinkProps: () => ({}),
}));

vi.mock('@app/recommendations/renderer/component/HoverEntityTooltip', () => ({
    HoverEntityTooltip: ({ children }: { children: React.ReactNode }) => <>{children}</>,
}));

vi.mock('@app/entityV2/shared/components/styled/DeprecationIcon', () => ({
    DeprecationIcon: () => <div data-testid="deprecation-icon" />,
}));

vi.mock('@app/entityV2/shared/links/DomainColoredIcon', () => ({
    DomainColoredIcon: () => null,
}));

vi.mock('@app/sharedV2/icons/PillRemoveIcon', () => ({
    default: () => null,
}));

const baseDomain = {
    urn: 'urn:li:domain:test',
    type: EntityType.Domain,
} as any;

const renderDomainLink = (domain: any) =>
    render(
        <ThemeProvider theme={themeV2}>
            <BrowserRouter>
                <DomainLink domain={domain} />
            </BrowserRouter>
        </ThemeProvider>,
    );

describe('DomainLink', () => {
    it('renders deprecation icon when domain is deprecated', () => {
        renderDomainLink({
            ...baseDomain,
            deprecation: { deprecated: true, note: 'Replaced', actor: null, decommissionTime: null },
        });
        expect(screen.getByTestId('deprecation-icon')).toBeInTheDocument();
    });

    it('does not render deprecation icon when deprecated=false', () => {
        renderDomainLink({
            ...baseDomain,
            deprecation: { deprecated: false, note: null, actor: null, decommissionTime: null },
        });
        expect(screen.queryByTestId('deprecation-icon')).not.toBeInTheDocument();
    });

    it('does not render deprecation icon when deprecation is null', () => {
        renderDomainLink({ ...baseDomain, deprecation: null });
        expect(screen.queryByTestId('deprecation-icon')).not.toBeInTheDocument();
    });

    it('does not render deprecation icon when deprecation is absent', () => {
        renderDomainLink({ ...baseDomain });
        expect(screen.queryByTestId('deprecation-icon')).not.toBeInTheDocument();
    });
});
