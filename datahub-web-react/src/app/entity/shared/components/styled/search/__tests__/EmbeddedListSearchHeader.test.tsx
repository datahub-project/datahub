import { MockedProvider } from '@apollo/client/testing';
import { render } from '@testing-library/react';
import React from 'react';

import EmbeddedListSearchHeader from '@app/entity/shared/components/styled/search/EmbeddedListSearchHeader';
import SearchExtendedMenu from '@app/entity/shared/components/styled/search/SearchExtendedMenu';
import TestPageContainer from '@src/utils/test-utils/TestPageContainer';

vi.mock('@app/entity/shared/components/styled/search/SearchExtendedMenu', () => ({
    default: vi.fn(() => null),
}));

describe('EmbeddedListSearchHeader', () => {
    // The CSV modal only shows download progress when it knows the total, so the header must forward it.
    it('passes the search total to the download menu', () => {
        render(
            <MockedProvider>
                <TestPageContainer>
                    <EmbeddedListSearchHeader
                        onSearch={vi.fn()}
                        onToggleFilters={vi.fn()}
                        downloadSearchResults={vi.fn()}
                        filters={[]}
                        query=""
                        isSelectMode={false}
                        isSelectAll={false}
                        selectedEntities={[]}
                        setIsSelectMode={vi.fn()}
                        onChangeSelectAll={vi.fn()}
                        numResults={1234}
                    />
                </TestPageContainer>
            </MockedProvider>,
        );

        expect(vi.mocked(SearchExtendedMenu)).toHaveBeenCalledWith(
            expect.objectContaining({ totalResults: 1234 }),
            expect.anything(),
        );
    });
});
