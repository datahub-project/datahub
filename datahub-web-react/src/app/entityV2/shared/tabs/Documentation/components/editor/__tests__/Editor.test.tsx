import { render } from '@testing-library/react';
import React from 'react';

import { Editor } from '@app/entityV2/shared/tabs/Documentation/components/editor/Editor';
import CustomThemeProvider from '@src/CustomThemeProvider';

// setupTests mocks Editor for all tests. We want to use the actual editor for this test module
vi.mock('../Editor', async () => vi.importActual('../Editor'));

describe('Editor', () => {
    it('should render the Editor and content without failure', () => {
        const content = 'testing the editor out';
        const { getByText } = render(
            <CustomThemeProvider>
                <Editor content={content} />
            </CustomThemeProvider>,
        );

        expect(getByText(content)).toBeInTheDocument();
        // loading the Editor in CI can be slow, extend the timeout beyond its usual 5 seconds to 20 seconds
    }, 20000);
});
