/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { MockedProvider } from '@apollo/client/testing';
import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import React from 'react';

import EmbeddedListSearchHeader from '@app/entityV2/shared/components/styled/search/EmbeddedListSearchHeader';
import { LineageTabContext } from '@src/app/entityV2/shared/tabs/Lineage/LineageTabContext';
import TestPageContainer from '@src/utils/test-utils/TestPageContainer';

import { LineageDirection, LineageSearchPath } from '@types';

describe('EmbeddedListSearchHeader', () => {
    const defaultProps = {
        onSearch: vi.fn(),
        onToggleFilters: vi.fn(),
        downloadSearchResults: vi.fn(),
        filters: [],
        query: '',
        isSelectMode: false,
        isSelectAll: false,
        selectedEntities: [],
        setSelectedEntities: vi.fn(),
        setIsSelectMode: vi.fn(),
        onChangeSelectAll: vi.fn(),
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
                        <EmbeddedListSearchHeader {...defaultProps} {...props} />
                    </LineageTabContext.Provider>
                </TestPageContainer>
            </MockedProvider>,
        );
    };

    describe('EmbeddedListSearchHeader', () => {
        it('should EmbeddedListSearchHeader when showLightningWarning is true and lineageSearchPath is LIGHTNING', () => {
            renderWithContext({ showLightningWarning: true }, { lineageSearchPath: LineageSearchPath.Lightning });

            expect(screen.getByTestId('lightning-cache-warning')).toBeInTheDocument();
        });

        it('should not show warning when lineageSearchPath is not LIGHTNING', () => {
            renderWithContext({ showLightningWarning: true }, { lineageSearchPath: LineageSearchPath.Tortoise });

            expect(screen.queryByTestId('lightning-cache-warning')).not.toBeInTheDocument();
        });

        it('should hide warning when close button is clicked', async () => {
            const user = userEvent.setup();

            renderWithContext({ showLightningWarning: true }, { lineageSearchPath: LineageSearchPath.Lightning });

            // Verify warning is initially shown
            expect(screen.getByTestId('lightning-cache-warning')).toBeInTheDocument();

            // Click the close button
            const closeButton = screen.getByTestId('close-lightning-cache-warning');
            await user.click(closeButton);

            // Verify warning is hidden
            expect(screen.queryByTestId('lightning-cache-warning')).not.toBeInTheDocument();
        });
    });
});
