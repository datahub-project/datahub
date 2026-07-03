import { render, screen } from '@testing-library/react';
import React from 'react';
import { BrowserRouter } from 'react-router-dom';
import { ThemeProvider } from 'styled-components';

import { useGlossaryEntityData } from '@app/entityV2/shared/GlossaryEntityContext';
import TermItem from '@app/glossaryV2/GlossaryBrowser/TermItem';
import themeV2 from '@conf/theme/themeV2';

import { EntityType } from '@types';

vi.mock('@app/useEntityRegistry', () => ({
    useEntityRegistry: () => ({
        getDisplayName: vi.fn(() => 'Adoptions'),
        getEntityUrl: vi.fn(() => '/glossaryTerm/test'),
    }),
}));

vi.mock('@app/entityV2/shared/components/styled/DeprecationIcon', () => ({
    DeprecationIcon: () => <div data-testid="deprecation-icon" />,
}));

vi.mock('@app/entityV2/shared/GlossaryEntityContext', () => ({
    useGlossaryEntityData: vi.fn(() => ({ entityData: null })),
}));

const baseTerm = {
    urn: 'urn:li:glossaryTerm:test',
    type: EntityType.GlossaryTerm,
    name: 'Adoptions',
    hierarchicalName: 'Adoptions.Adoptions',
    properties: { name: 'Adoptions', description: null },
    domain: null,
} as any;

const renderTermItem = (term: any) =>
    render(
        <ThemeProvider theme={themeV2}>
            <BrowserRouter>
                <TermItem term={term} depth={0} />
            </BrowserRouter>
        </ThemeProvider>,
    );

describe('TermItem — deprecation badge', () => {
    it('renders deprecation icon when term is deprecated', () => {
        renderTermItem({
            ...baseTerm,
            deprecation: { deprecated: true, note: 'Replaced', actor: null, decommissionTime: null },
        });
        expect(screen.getByTestId('deprecation-icon')).toBeInTheDocument();
    });

    it('does not render deprecation icon when deprecated is false', () => {
        renderTermItem({
            ...baseTerm,
            deprecation: { deprecated: false, note: null, actor: null, decommissionTime: null },
        });
        expect(screen.queryByTestId('deprecation-icon')).not.toBeInTheDocument();
    });

    it('does not render deprecation icon when deprecation is null', () => {
        renderTermItem({ ...baseTerm, deprecation: null });
        expect(screen.queryByTestId('deprecation-icon')).not.toBeInTheDocument();
    });

    it('does not render deprecation icon when deprecation is absent', () => {
        renderTermItem({ ...baseTerm });
        expect(screen.queryByTestId('deprecation-icon')).not.toBeInTheDocument();
    });
});

describe('TermItem — live deprecation state when this row is the open entity', () => {
    it('shows the badge from entityData even when the sidebar-fetched term is not deprecated', () => {
        vi.mocked(useGlossaryEntityData).mockReturnValueOnce({
            entityData: { urn: baseTerm.urn, deprecation: { deprecated: true, note: '', decommissionTime: null } },
        } as any);
        renderTermItem({ ...baseTerm, deprecation: { deprecated: false, note: null, actor: null, decommissionTime: null } });
        expect(screen.getByTestId('deprecation-icon')).toBeInTheDocument();
    });

    it('clears a stale badge when entityData shows un-deprecated but the sidebar fetch is still deprecated', () => {
        vi.mocked(useGlossaryEntityData).mockReturnValueOnce({
            entityData: { urn: baseTerm.urn, deprecation: { deprecated: false, note: null, decommissionTime: null } },
        } as any);
        renderTermItem({ ...baseTerm, deprecation: { deprecated: true, note: 'stale', actor: null, decommissionTime: null } });
        expect(screen.queryByTestId('deprecation-icon')).not.toBeInTheDocument();
    });

    it('falls back to the term prop when entityData belongs to a different entity', () => {
        vi.mocked(useGlossaryEntityData).mockReturnValueOnce({
            entityData: {
                urn: 'urn:li:glossaryTerm:someOtherTerm',
                deprecation: { deprecated: false, note: null, decommissionTime: null },
            },
        } as any);
        renderTermItem({ ...baseTerm, deprecation: { deprecated: true, note: null, actor: null, decommissionTime: null } });
        expect(screen.getByTestId('deprecation-icon')).toBeInTheDocument();
    });
});
