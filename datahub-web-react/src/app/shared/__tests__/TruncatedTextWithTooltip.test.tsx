import React from 'react';
import { render } from '@testing-library/react';
import { TruncatedTextWithTooltip } from '../TruncatedTextWithTooltip';

describe('TruncatedTextWithTooltip', () => {
    it('renders truncated text when text is longer than maxLength', () => {
        const { getByText } = render(<TruncatedTextWithTooltip text="This is some long text" maxLength={10} />);
        expect(getByText('This is so...')).toBeInTheDocument();
    });

    it('renders full text when text is shorter than maxLength', () => {
        const { getByText } = render(<TruncatedTextWithTooltip text="This is some short text" maxLength={23} />);
        expect(getByText('This is some short text')).toBeInTheDocument();
    });

    it('renders custom text when renderText is provided', () => {
        const { getByText, getByTestId } = render(
            <TruncatedTextWithTooltip
                text="This is some long text"
                maxLength={10}
                renderText={(truncatedText) => <span data-testid="render-text-span">{truncatedText}</span>}
            />,
        );
        expect(getByText('This is so...')).toBeInTheDocument();
        expect(getByTestId('render-text-span')).toBeInTheDocument();
    });
});
