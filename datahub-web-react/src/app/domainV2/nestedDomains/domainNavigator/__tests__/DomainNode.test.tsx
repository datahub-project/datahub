import { render, screen } from '@testing-library/react';
import React from 'react';
import { ThemeProvider } from 'styled-components';

import DomainNode from '@app/domainV2/nestedDomains/domainNavigator/DomainNode';
import themeV2 from '@conf/theme/themeV2';

import { EntityType } from '@types';

// Hoisted so they can be overridden per-test with mockReturnValueOnce
const mockUseScrollDomains = vi.hoisted(() => vi.fn());
const mockUseToggle = vi.hoisted(() => vi.fn());

vi.mock('@app/useEntityRegistry', () => ({
    useEntityRegistry: () => ({
        getDisplayName: vi.fn(() => 'Test Domain'),
        getEntityUrl: vi.fn(() => '/domain/test'),
    }),
}));

vi.mock('@app/domainV2/DomainsContext', () => ({
    useDomainsContext: () => ({ entityData: null }),
}));

vi.mock('react-router', () => ({
    useHistory: () => ({ push: vi.fn() }),
}));

vi.mock('@app/domainV2/useScrollDomains', () => ({
    default: mockUseScrollDomains,
}));

vi.mock('@app/shared/useToggle', () => ({
    default: mockUseToggle,
}));

vi.mock('@app/entityV2/shared/components/styled/DeprecationIcon', () => ({
    DeprecationIcon: () => <div data-testid="deprecation-icon" />,
}));

vi.mock('@app/entityV2/shared/links/DomainColoredIcon', () => ({
    DomainColoredIcon: () => null,
}));

vi.mock('@components', () => ({
    Pill: () => null,
    Tooltip: ({ children }: { children: React.ReactNode }) => <>{children}</>,
}));

vi.mock('@app/sharedV2/sidebar/components', () => ({
    RotatingTriangle: () => null,
}));

vi.mock('@app/shared/Loading', () => ({
    default: () => null,
}));

vi.mock('@app/shared/components', () => ({
    BodyContainer: ({ children }: { children: React.ReactNode }) => <>{children}</>,
    BodyGridExpander: ({ children }: { children: React.ReactNode }) => <>{children}</>,
}));

const closedToggle = {
    isOpen: false,
    isClosing: false,
    toggle: vi.fn(),
    toggleOpen: vi.fn(),
    toggleClose: vi.fn(),
};

const openToggle = { ...closedToggle, isOpen: true };

const noChildren = { domains: [], loading: false, scrollRef: { current: null } };

const baseDomain = {
    urn: 'urn:li:domain:test',
    type: EntityType.Domain,
    children: { total: 0 },
} as any;

beforeEach(() => {
    mockUseToggle.mockReturnValue(closedToggle);
    mockUseScrollDomains.mockReturnValue(noChildren);
});

const renderDomainNode = (domain: any, props: Record<string, any> = {}) =>
    render(
        <ThemeProvider theme={themeV2}>
            <DomainNode domain={domain} numDomainChildren={0} {...props} />
        </ThemeProvider>,
    );

describe('DomainNode — deprecation badge', () => {
    it('renders deprecation icon when domain is deprecated and sidebar is not collapsed', () => {
        renderDomainNode({
            ...baseDomain,
            deprecation: { deprecated: true, note: 'Replaced', actor: null, decommissionTime: null },
        });
        expect(screen.getByTestId('deprecation-icon')).toBeInTheDocument();
    });

    it('does not render deprecation icon when deprecated is false', () => {
        renderDomainNode({
            ...baseDomain,
            deprecation: { deprecated: false, note: null, actor: null, decommissionTime: null },
        });
        expect(screen.queryByTestId('deprecation-icon')).not.toBeInTheDocument();
    });

    it('does not render deprecation icon when deprecation is null', () => {
        renderDomainNode({ ...baseDomain, deprecation: null });
        expect(screen.queryByTestId('deprecation-icon')).not.toBeInTheDocument();
    });

    it('does not render deprecation icon when sidebar is collapsed', () => {
        renderDomainNode(
            {
                ...baseDomain,
                deprecation: { deprecated: true, note: null, actor: null, decommissionTime: null },
            },
            { isCollapsed: true },
        );
        expect(screen.queryByTestId('deprecation-icon')).not.toBeInTheDocument();
    });
});

describe('DomainNode — child rendering', () => {
    it('renders all child DomainNodes when expanded and does not emit a React key warning', () => {
        const consoleError = vi.spyOn(console, 'error');

        const childDomains = [
            { urn: 'urn:li:domain:child-1', type: EntityType.Domain, children: { total: 0 } },
            { urn: 'urn:li:domain:child-2', type: EntityType.Domain, children: { total: 0 } },
            { urn: 'urn:li:domain:child-3', type: EntityType.Domain, children: { total: 0 } },
        ];

        // Parent is open; children are closed and have no grandchildren
        mockUseToggle
            .mockReturnValueOnce(openToggle) // parent
            .mockReturnValue(closedToggle); // each child

        mockUseScrollDomains
            .mockReturnValueOnce({ domains: childDomains, loading: false, scrollRef: { current: null } }) // parent's children
            .mockReturnValue(noChildren); // grandchildren (empty)

        renderDomainNode(baseDomain, { numDomainChildren: 3 });

        // 1 parent + 3 children = 4 domain rows total
        expect(screen.getAllByTestId('domain-options-list')).toHaveLength(4);

        // No React duplicate-key warning should have fired
        const keyWarning = consoleError.mock.calls.find((args) => String(args[0]).toLowerCase().includes('key'));
        expect(keyWarning).toBeUndefined();

        consoleError.mockRestore();
    });
});
