import React from 'react';
import { render } from '@testing-library/react';
import { BrowserRouter } from 'react-router-dom';
import { LinkWrapper } from '../LinkWrapper';

describe('LinkWrapper', () => {
    it('renders absolute URLs', () => {
        const { getByRole } = render(<LinkWrapper to="https://datahubproject.io/" />);

        const link = getByRole('link');
        expect(link).toBeInTheDocument();
        expect(link).toHaveAttribute('href', 'https://datahubproject.io/');
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
