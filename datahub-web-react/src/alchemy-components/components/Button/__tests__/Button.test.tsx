import { render, screen } from '@testing-library/react';
import React from 'react';
import { ThemeProvider } from 'styled-components';
import { describe, expect, it } from 'vitest';

import { Button } from '@components/components/Button/Button';

import themeV2 from '@conf/theme/themeV2';

function renderButton(isLoading: boolean) {
    return (
        <ThemeProvider theme={themeV2}>
            <Button isLoading={isLoading} data-testid="button">
                Submit
            </Button>
        </ThemeProvider>
    );
}

describe('Button', () => {
    // Firefox never paints a text node that was whitespace-only inside a flex container and later
    // gets text. If the loading state renders one, React reuses it for the label once loading ends
    // and the button collapses to its padding.
    it('should not render whitespace-only text nodes while loading', () => {
        render(renderButton(true));

        const textNodes = Array.from(screen.getByTestId('button').childNodes).filter(
            (node) => node.nodeType === Node.TEXT_NODE,
        );
        expect(textNodes.map((node) => node.textContent)).toEqual(['Submit']);
    });

    it('should keep the label after loading ends', () => {
        const { rerender } = render(renderButton(true));
        rerender(renderButton(false));

        expect(screen.getByTestId('button')).toHaveTextContent('Submit');
    });
});
