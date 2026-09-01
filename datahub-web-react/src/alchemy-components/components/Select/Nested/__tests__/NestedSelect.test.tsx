import { render, screen } from '@testing-library/react';
import React from 'react';
import { ThemeProvider } from 'styled-components';
import { describe, expect, it } from 'vitest';

import { NestedSelect } from '@components/components/Select/Nested/NestedSelect';

import themeV2 from '@conf/theme/themeV2';

describe('NestedSelect', () => {
    it('renders label without asterisk when isRequired is false', () => {
        render(
            <ThemeProvider theme={themeV2}>
                <NestedSelect label="Domain" options={[]} isRequired={false} />
            </ThemeProvider>,
        );

        expect(screen.getByText('Domain')).toBeInTheDocument();
        expect(screen.queryByText('*')).not.toBeInTheDocument();
    });

    it('renders label with asterisk when isRequired is true', () => {
        render(
            <ThemeProvider theme={themeV2}>
                <NestedSelect label="Domain" options={[]} isRequired />
            </ThemeProvider>,
        );

        expect(screen.getByText('Domain')).toBeInTheDocument();
        expect(screen.getByText('*')).toBeInTheDocument();
    });
});
