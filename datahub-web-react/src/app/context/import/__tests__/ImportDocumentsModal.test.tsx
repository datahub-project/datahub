import { MockedProvider } from '@apollo/client/testing';
import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import React from 'react';
import { describe, expect, it, vi } from 'vitest';

import ImportDocumentsModal from '@app/context/import/ImportDocumentsModal';
import '@app/context/import/__tests__/testSetup';
import { DocumentTreeContext, DocumentTreeNode } from '@app/document/DocumentTreeContext';
import CustomThemeProvider from '@src/CustomThemeProvider';

const mockLaunchGitHub = vi.fn();

vi.mock('@app/context/import/hooks/useLaunchGitHubDocumentsIngestion', () => ({
    useLaunchGitHubDocumentsIngestion: () => mockLaunchGitHub,
}));

vi.mock('@app/context/useUserContext', () => ({
    useUserContext: () => ({
        platformPrivileges: { manageIngestion: true },
    }),
}));

vi.mock('@app/useAppConfig', () => ({
    useAppConfig: () => ({
        featureFlags: { showIngestionPageRedesign: true },
    }),
}));

const mockDocumentTreeContextValue = {
    nodes: new Map<string, DocumentTreeNode>(),
    rootUrns: [],
    expandedUrns: new Set<string>(),
    getNode: vi.fn(),
    getRootNodes: vi.fn().mockReturnValue([]),
    getChildren: vi.fn().mockReturnValue([]),
    updateNodeTitle: vi.fn(),
    moveNode: vi.fn(),
    deleteNode: vi.fn(),
    addNode: vi.fn(),
    setNodeChildren: vi.fn(),
    initializeTree: vi.fn(),
    setExpandedUrns: vi.fn(),
    toggleExpanded: vi.fn(),
    expandNode: vi.fn(),
    collapseNode: vi.fn(),
};

const Wrapper: React.FC<{ children: React.ReactNode }> = ({ children }) => (
    <MockedProvider mocks={[]} addTypename={false}>
        <CustomThemeProvider>
            <DocumentTreeContext.Provider value={mockDocumentTreeContextValue}>{children}</DocumentTreeContext.Provider>
        </CustomThemeProvider>
    </MockedProvider>
);

describe('ImportDocumentsModal', () => {
    it('does not render when not visible', () => {
        render(
            <Wrapper>
                <ImportDocumentsModal visible={false} onClose={vi.fn()} />
            </Wrapper>,
        );

        expect(screen.queryByText('Import Documents')).not.toBeInTheDocument();
    });

    it('renders source selection step when visible', () => {
        render(
            <Wrapper>
                <ImportDocumentsModal visible onClose={vi.fn()} />
            </Wrapper>,
        );

        expect(screen.getByText('Import Documents')).toBeInTheDocument();
        expect(screen.getByText('Upload Files')).toBeInTheDocument();
        expect(screen.getByText('GitHub Repository')).toBeInTheDocument();
    });

    it('navigates to file upload configuration when Upload Files is clicked', async () => {
        render(
            <Wrapper>
                <ImportDocumentsModal visible onClose={vi.fn()} />
            </Wrapper>,
        );

        fireEvent.click(screen.getByText('Upload Files'));

        await waitFor(() => {
            expect(screen.getByText(/Supported formats:/)).toBeInTheDocument();
        });
    });

    it('launches ingestion setup when GitHub Repository is selected', async () => {
        const onClose = vi.fn();
        render(
            <Wrapper>
                <ImportDocumentsModal visible onClose={onClose} />
            </Wrapper>,
        );

        fireEvent.click(screen.getByText('GitHub Repository'));

        await waitFor(() => {
            expect(screen.getByText('Continue to setup')).toBeInTheDocument();
        });

        fireEvent.click(screen.getByText('Continue to setup'));

        expect(mockLaunchGitHub).toHaveBeenCalled();
        expect(onClose).toHaveBeenCalled();
    });

    it('shows parent selector in configure step', async () => {
        render(
            <Wrapper>
                <ImportDocumentsModal visible onClose={vi.fn()} />
            </Wrapper>,
        );

        fireEvent.click(screen.getByText('Upload Files'));

        await waitFor(() => {
            expect(screen.getByText('Import into:')).toBeInTheDocument();
        });
    });

    it('shows Cancel button on source step', () => {
        render(
            <Wrapper>
                <ImportDocumentsModal visible onClose={vi.fn()} />
            </Wrapper>,
        );

        expect(screen.getByText('Cancel')).toBeInTheDocument();
    });

    it('calls onClose when Cancel is clicked', () => {
        const onClose = vi.fn();

        render(
            <Wrapper>
                <ImportDocumentsModal visible onClose={onClose} />
            </Wrapper>,
        );

        fireEvent.click(screen.getByText('Cancel'));
        expect(onClose).toHaveBeenCalled();
    });
});
