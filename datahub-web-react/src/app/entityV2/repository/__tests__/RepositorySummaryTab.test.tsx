import { render, screen } from '@testing-library/react';
import React from 'react';
import { MemoryRouter } from 'react-router-dom';
import { describe, expect, it, vi } from 'vitest';

import { RepositorySummaryTab } from '@app/entityV2/repository/RepositorySummaryTab';
import CustomThemeProvider from '@src/CustomThemeProvider';

const mockUseBaseEntity = vi.fn();
vi.mock('@app/entity/shared/EntityContext', () => ({
    useBaseEntity: () => mockUseBaseEntity(),
}));
vi.mock('@app/useEntityRegistry', () => ({
    useEntityRegistry: () => ({ getEntityUrl: (_type: unknown, urn: string) => `/entity/${urn}` }),
}));
vi.mock('react-i18next', () => ({
    useTranslation: () => ({ t: (key: string) => key }),
}));
vi.mock('@app/entityV2/shared/summary/SummaryAboutSection', () => ({ default: () => <div data-testid="about" /> }));

function renderTab(repository: unknown) {
    mockUseBaseEntity.mockReturnValue({ repository });
    return render(
        <MemoryRouter>
            <CustomThemeProvider>
                <RepositorySummaryTab />
            </CustomThemeProvider>
        </MemoryRouter>,
    );
}

describe('RepositorySummaryTab', () => {
    it('renders facts and the source link', () => {
        renderTab({
            properties: {
                languages: ['Python', 'TypeScript'],
                license: 'Apache-2.0',
                defaultBranch: 'main',
            },
            source: { externalUrl: 'https://github.com/acme/repo' },
        });

        expect(screen.getByText('Python')).toBeInTheDocument();
        expect(screen.getByText('TypeScript')).toBeInTheDocument();
        expect(screen.getByText('Apache-2.0')).toBeInTheDocument();
        // External source link.
        const src = screen.getByText('repository.summary.viewSource').closest('a');
        expect(src).toHaveAttribute('href', 'https://github.com/acme/repo');
    });

    it('renders the about section', () => {
        renderTab({ properties: { languages: ['Go'] } });

        expect(screen.getByTestId('about')).toBeInTheDocument();
    });

    it('renders created and lastModified facts when present', () => {
        renderTab({
            properties: {
                languages: ['Go'],
                created: { time: new Date('2020-01-01').getTime(), actor: 'urn:li:corpuser:jdoe' },
                lastModified: { time: new Date('2020-06-01').getTime(), actor: 'urn:li:corpuser:jdoe' },
            },
        });

        expect(screen.getByText(/repository\.summary\.created/)).toBeInTheDocument();
        expect(screen.getByText(/repository\.summary\.lastModified/)).toBeInTheDocument();
    });

    it('omits created and lastModified facts when absent', () => {
        renderTab({ properties: { languages: ['Go'] } });

        expect(screen.queryByText(/repository\.summary\.created/)).not.toBeInTheDocument();
        expect(screen.queryByText(/repository\.summary\.lastModified/)).not.toBeInTheDocument();
    });
});
