import React from 'react';
import { render } from '@testing-library/react';
import { Editor } from '../Editor';

// setupTests mocks Editor for all tests. We want to use the actual editor for this test module
vi.mock('../Editor', async () => vi.importActual('../Editor'));

// TODO: Fix this test, it's breaking because `useRemirrorContext` was called outside of the `remirror` context. It can only be used within an active remirror context created `<Remirror />`.
describe.skip('Editor', () => {
    it('should render the Editor and content without failure', () => {
        const content = 'testing the editor out';
        const { getByText } = render(<Editor content={content} />);

        expect(getByText(content)).toBeInTheDocument();
        // loading the Editor in CI can be slow, extend the timeout beyond its usual 5 seconds to 20 seconds
    }, 20000);
});
