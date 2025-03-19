import React from 'react';
import { render, screen } from '@testing-library/react';
import { LineageTabContext } from '@src/app/entityV2/shared/tabs/Lineage/LineageTabContext';
import TestPageContainer from '@src/utils/test-utils/TestPageContainer';
import { MockedProvider } from '@apollo/client/testing';
import { LineageDirection, LineageSearchPath } from '../../../../../../../types.generated';
import DownloadAsCsvModal from '../DownloadAsCsvModal';

describe('DownloadAsCsvModal', () => {
    const defaultProps = {
        downloadSearchResults: vi.fn().mockResolvedValue(null),
        filters: [],
        query: '',
        setIsDownloadingCsv: vi.fn(),
        showDownloadAsCsvModal: true,
        setShowDownloadAsCsvModal: vi.fn(),
    };

    const renderWithContext = (props = {}, contextValue = {}) => {
        const defaultContextValue = {
            isColumnLevelLineage: false,
            lineageDirection: LineageDirection.Downstream,
            selectedColumn: undefined,
            lineageSearchPath: null,
            setLineageSearchPath: vi.fn(),
        };

        return render(
            <MockedProvider>
                <TestPageContainer>
                    <LineageTabContext.Provider value={{ ...defaultContextValue, ...contextValue }}>
                        <DownloadAsCsvModal {...defaultProps} {...props} />
                    </LineageTabContext.Provider>
                </TestPageContainer>
            </MockedProvider>,
        );
    };

    describe('Impact Analysis Warning', () => {
        it('should show warning when lineageSearchPath is LIGHTNING', () => {
            renderWithContext({}, { lineageSearchPath: LineageSearchPath.Lightning });

            expect(screen.getByTestId('lightning-cache-warning')).toBeInTheDocument();
        });

        it('should not show warning when lineageSearchPath is not LIGHTNING', () => {
            renderWithContext({}, { lineageSearchPath: LineageSearchPath.Tortoise });

            expect(screen.queryByTestId('lightning-cache-warning')).not.toBeInTheDocument();
        });

        it('should not show warning when lineageSearchPath is null', () => {
            renderWithContext({}, { lineageSearchPath: null });

            expect(screen.queryByTestId('lightning-cache-warning')).not.toBeInTheDocument();
        });
    });
});
