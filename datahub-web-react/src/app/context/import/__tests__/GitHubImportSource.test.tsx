import { render, screen } from '@testing-library/react';
import React from 'react';
import { describe, expect, it, vi } from 'vitest';

import '@app/context/import/__tests__/testSetup';
import type { GitHubImportConfig } from '@app/context/import/hooks/useImportFromGitHub';
import GitHubImportSource from '@app/context/import/sources/GitHubImportSource';
import CustomThemeProvider from '@src/CustomThemeProvider';

const Wrapper: React.FC<{ children: React.ReactNode }> = ({ children }) => (
    <CustomThemeProvider>{children}</CustomThemeProvider>
);

const DEFAULT_CONFIG: GitHubImportConfig = {
    repoUrl: '',
    branch: 'main',
    path: '',
    fileExtensions: ['.md', '.txt'],
    githubToken: '',
    showInGlobalContext: true,
};

describe('GitHubImportSource', () => {
    it('renders all form fields', () => {
        render(
            <Wrapper>
                <GitHubImportSource config={DEFAULT_CONFIG} onChange={vi.fn()} />
            </Wrapper>,
        );

        expect(screen.getByText('Repository')).toBeInTheDocument();
        expect(screen.getByText('Branch')).toBeInTheDocument();
        expect(screen.getByText('Path (optional)')).toBeInTheDocument();
        expect(screen.getByText('File Extensions')).toBeInTheDocument();
        expect(screen.getByText('Personal Access Token')).toBeInTheDocument();
    });

    it('shows PAT permissions hint', () => {
        render(
            <Wrapper>
                <GitHubImportSource config={DEFAULT_CONFIG} onChange={vi.fn()} />
            </Wrapper>,
        );

        expect(screen.getByText(/Contents.*read permission/)).toBeInTheDocument();
        expect(screen.getByText(/Not stored/)).toBeInTheDocument();
    });

    it('displays current config values', () => {
        const config: GitHubImportConfig = {
            ...DEFAULT_CONFIG,
            repoUrl: 'acme/docs',
            branch: 'develop',
            path: 'docs/',
            fileExtensions: ['.md'],
            githubToken: 'ghp_secret',
        };

        render(
            <Wrapper>
                <GitHubImportSource config={config} onChange={vi.fn()} />
            </Wrapper>,
        );

        const inputs = screen.getAllByRole('textbox');
        const repoInput = inputs.find((i) => (i as HTMLInputElement).value === 'acme/docs');
        expect(repoInput).toBeDefined();
    });
});
