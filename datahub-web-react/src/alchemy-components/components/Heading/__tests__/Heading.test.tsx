import { render, screen } from '@testing-library/react';
import React, { createRef } from 'react';
import styled, { ThemeProvider } from 'styled-components';
import { describe, expect, it } from 'vitest';

import { Heading } from '@components/components/Heading/Heading';

import themeV2 from '@conf/theme/themeV2';

const SpacedHeading = styled(Heading)`
    margin-bottom: 8px;
`;

describe('Heading', () => {
    it('applies styles from styled(Heading)', () => {
        render(
            <ThemeProvider theme={themeV2}>
                <SpacedHeading type="h5" data-testid="heading">
                    Title
                </SpacedHeading>
            </ThemeProvider>,
        );

        const heading = screen.getByTestId('heading');
        expect(heading.tagName).toBe('H5');
        expect(heading).toHaveStyle({ marginBottom: '8px' });
    });

    it('forwards its ref to the heading element', () => {
        const ref = createRef<HTMLHeadingElement>();
        render(
            <ThemeProvider theme={themeV2}>
                <Heading type="h3" ref={ref}>
                    Title
                </Heading>
            </ThemeProvider>,
        );

        expect(ref.current?.tagName).toBe('H3');
    });
});
