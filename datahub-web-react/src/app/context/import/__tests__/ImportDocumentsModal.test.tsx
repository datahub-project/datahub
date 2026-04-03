import { MockedProvider } from '@apollo/client/testing';
import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import React from 'react';
import { describe, expect, it, vi } from 'vitest';

import ImportDocumentsModal from '@app/context/import/ImportDocumentsModal';
import '@app/context/import/__tests__/testSetup';
import { DocumentTreeContext, DocumentTreeNode } from '@app/document/DocumentTreeContext';
import CustomThemeProvider from '@src/CustomThemeProvider';

const mockDocumentTreeContextValue = {
    nodes: new Map<string, DocumentTreeNode>(),
    rootUrns: [],
    expandedUrns: new Set<string>(),
    expandNode: vi.fn(),
    collapseNode: vi.fn(),
    toggleNode: vi.fn(),
    getNode: vi.fn(),
    setNodes: vi.fn(),
    setRootUrns: vi.fn(),
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

    it('navigates to GitHub configuration when GitHub Repository is clicked', async () => {
        render(
            <Wrapper>
                <ImportDocumentsModal visible onClose={vi.fn()} />
            </Wrapper>,
        );

        fireEvent.click(screen.getByText('GitHub Repository'));

        await waitFor(() => {
            expect(screen.getByText('Repository')).toBeInTheDocument();
            expect(screen.getByText('Personal Access Token')).toBeInTheDocument();
        });
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

    it('shows Back and Import buttons on configure step', async () => {
        render(
            <Wrapper>
                <ImportDocumentsModal visible onClose={vi.fn()} />
            </Wrapper>,
        );

        fireEvent.click(screen.getByText('Upload Files'));

        await waitFor(() => {
            expect(screen.getByText('Back')).toBeInTheDocument();
            expect(screen.getByText('Import')).toBeInTheDocument();
        });
    });

    it('Back button returns to source selection', async () => {
        render(
            <Wrapper>
                <ImportDocumentsModal visible onClose={vi.fn()} />
            </Wrapper>,
        );

        fireEvent.click(screen.getByText('Upload Files'));

        await waitFor(() => {
            expect(screen.getByText('Back')).toBeInTheDocument();
        });

        fireEvent.click(screen.getByText('Back'));

        await waitFor(() => {
            expect(screen.getByText('Upload Files')).toBeInTheDocument();
            expect(screen.getByText('GitHub Repository')).toBeInTheDocument();
        });
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
