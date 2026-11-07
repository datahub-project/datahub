import { render } from '@testing-library/react';
import React, { createRef } from 'react';
import { ThemeProvider } from 'styled-components';
import { describe, expect, it } from 'vitest';

import { Text } from '@components/components/Text/Text';

import themeV2 from '@conf/theme/themeV2';

describe('Text', () => {
    // Tooltip and Popover position themselves from their child's DOM node, which they reach through a ref.
    it('forwards its ref to the rendered element', () => {
        const ref = createRef<HTMLElement>();
        render(
            <ThemeProvider theme={themeV2}>
                <Text type="span" ref={ref}>
                    Label
                </Text>
            </ThemeProvider>,
        );

        expect(ref.current?.tagName).toBe('SPAN');
        expect(ref.current).toHaveTextContent('Label');
    });
});
