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
import { BrowserRouter } from 'react-router-dom';

import { LinkWrapper } from '@app/shared/LinkWrapper';

describe('LinkWrapper', () => {
    it('renders absolute URLs', () => {
        const { getByRole } = render(<LinkWrapper to="https://docs.datahub.com/" />);

        const link = getByRole('link');
        expect(link).toBeInTheDocument();
        expect(link).toHaveAttribute('href', 'https://docs.datahub.com/');
    });

    it('renders relative URLs', () => {
        const { getByRole } = render(
            <BrowserRouter>
                <LinkWrapper to="/some/relative/path" />
            </BrowserRouter>,
        );

        const link = getByRole('link');
        expect(link).toBeInTheDocument();
        expect(link).toHaveAttribute('href', '/some/relative/path');
    });

    it('renders children when URL is falsy', () => {
        const { queryByRole, getByTestId } = render(
            <LinkWrapper to={null}>
                <div data-testid="child-element" />
            </LinkWrapper>,
        );

        expect(queryByRole('link')).not.toBeInTheDocument();
        expect(getByTestId('child-element')).toBeInTheDocument();
    });
});
