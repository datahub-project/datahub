import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import React from 'react';
import { ThemeProvider } from 'styled-components';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { NestedSelect } from '@components/components/Select/Nested/NestedSelect';
import { NestedSelectOption } from '@components/components/Select/Nested/types';

import themeV2 from '@conf/theme/themeV2';
import { mockVisibilityObserver } from '@utils/test-utils/mockVisibilityObserver';

const OPTION_A: NestedSelectOption = { value: 'a', label: 'Option A' };
const OPTION_B: NestedSelectOption = { value: 'b', label: 'Option B' };

function renderNestedSelect(props: Record<string, unknown>) {
    return render(
        <ThemeProvider theme={themeV2}>
            <NestedSelect dataTestId="nested" options={[OPTION_A, OPTION_B]} {...props} />
        </ThemeProvider>,
    );
}

describe('NestedSelect', () => {
    beforeEach(() => {
        mockVisibilityObserver();
    });

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

    it('replaces the previous selection in single select mode', async () => {
        const onUpdate = vi.fn();
        renderNestedSelect({ isMultiSelect: false, initialValues: [OPTION_A], onUpdate });

        await userEvent.click(screen.getByTestId('nested-base'));
        await userEvent.click(screen.getByTestId('child-option-b'));

        expect(onUpdate).toHaveBeenLastCalledWith([OPTION_B]);
    });

    it('keeps a single select pick staged until confirmed when a confirmation footer is shown', async () => {
        const onUpdate = vi.fn();
        renderNestedSelect({
            isMultiSelect: false,
            shouldDisplayConfirmationFooter: true,
            initialValues: [OPTION_A],
            onUpdate,
        });

        await userEvent.click(screen.getByTestId('nested-base'));
        await userEvent.click(screen.getByTestId('child-option-b'));

        expect(onUpdate).not.toHaveBeenCalled();

        await userEvent.click(screen.getByTestId('footer-button-update'));

        expect(onUpdate).toHaveBeenCalledWith([OPTION_B]);
    });
});
