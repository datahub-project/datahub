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

import { TruncatedTextWithTooltip } from '@app/shared/TruncatedTextWithTooltip';

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
