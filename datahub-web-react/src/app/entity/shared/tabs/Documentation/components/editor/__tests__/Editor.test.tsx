import React from 'react';

import { Editor } from '@app/entity/shared/tabs/Documentation/components/editor/Editor';
import { render } from '@utils/test-utils/customRender';

// setupTests mocks Editor for all tests. We want to use the actual editor for this test module
vi.mock('../Editor', async () => vi.importActual('../Editor'));

vi.mock('@graphql/search.generated', () => ({
    useGetAutoCompleteMultipleResultsLazyQuery: vi.fn(() => [
        vi.fn(), // mock query function
        { loading: false, error: null, data: [] }, // mock result
    ]),
}));

describe('Editor', () => {
    it('should render the Editor and content without failure', () => {
        const content = 'testing the editor out';
        const { getByText } = render(<Editor content={content} />);

        expect(getByText(content)).toBeInTheDocument();
        // loading the Editor in CI can be slow, extend the timeout beyond its usual 5 seconds to 20 seconds
    }, 20000);
});
