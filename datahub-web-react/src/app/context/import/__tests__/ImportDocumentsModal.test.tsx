import { MockedProvider } from '@apollo/client/testing';
import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import React from 'react';
import { describe, expect, it, vi } from 'vitest';

import ImportDocumentsModal from '@app/context/import/ImportDocumentsModal';
import '@app/context/import/__tests__/testSetup';
import CustomThemeProvider from '@src/CustomThemeProvider';

const mockLaunchDocumentIngestion = vi.fn();

vi.mock('@app/context/import/hooks/useLaunchDocumentIngestionSource', () => ({
    useLaunchDocumentIngestionSource: () => mockLaunchDocumentIngestion,
}));

vi.mock('@app/context/useUserContext', () => ({
    useUserContext: () => ({
        platformPrivileges: { manageIngestion: true },
    }),
}));

vi.mock('@app/useAppConfig', () => ({
    useAppConfig: () => ({
        config: {
            featureFlags: { showIngestionPageRedesign: true },
        },
        loaded: true,
    }),
}));

const Wrapper: React.FC<{ children: React.ReactNode }> = ({ children }) => (
    <MockedProvider mocks={[]} addTypename={false}>
        <CustomThemeProvider>{children}</CustomThemeProvider>
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
        expect(screen.getByText('GitHub')).toBeInTheDocument();
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

    it('launches ingestion setup immediately when GitHub is selected', () => {
        const onClose = vi.fn();
        render(
            <Wrapper>
                <ImportDocumentsModal visible onClose={onClose} />
            </Wrapper>,
        );

        fireEvent.click(screen.getByText('GitHub'));

        expect(mockLaunchDocumentIngestion).toHaveBeenCalledWith({ source: 'GITHUB' });
        expect(onClose).toHaveBeenCalled();
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
