/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { render } from '@testing-library/react';
import React from 'react';

import { Editor } from '@app/entity/shared/tabs/Documentation/components/editor/Editor';

// setupTests mocks Editor for all tests. We want to use the actual editor for this test module
vi.mock('../Editor', async () => vi.importActual('../Editor'));

describe('Editor', () => {
    it('should render the Editor and content without failure', () => {
        const content = 'testing the editor out';
        const { getByText } = render(<Editor content={content} />);

        expect(getByText(content)).toBeInTheDocument();
        // loading the Editor in CI can be slow, extend the timeout beyond its usual 5 seconds to 20 seconds
    }, 20000);
});
