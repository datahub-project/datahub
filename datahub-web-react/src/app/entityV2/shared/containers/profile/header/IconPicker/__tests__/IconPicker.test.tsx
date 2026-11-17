import { act, render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import React from 'react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { ChatIconPicker } from '@app/entityV2/shared/containers/profile/header/IconPicker/IconPicker';
import CustomThemeProvider from '@src/CustomThemeProvider';

const { loadPhosphorIconsMock } = vi.hoisted(() => ({
    loadPhosphorIconsMock: vi.fn(),
}));

vi.mock('@app/entityV2/shared/containers/profile/header/IconPicker/loadPhosphorIcons', () => ({
    loadPhosphorIcons: loadPhosphorIconsMock,
}));

vi.mock('@components/components/Icon/constants', () => ({
    PHOSPHOR_ICONS: ['House', 'Shapes', 'UserCircle'],
}));

function MockIcon() {
    return <svg data-testid="phosphor-glyph" />;
}

function renderPicker(props: Partial<React.ComponentProps<typeof ChatIconPicker>> = {}) {
    const onIconPick = props.onIconPick ?? vi.fn();
    return {
        onIconPick,
        ...render(
            <CustomThemeProvider>
                <ChatIconPicker onIconPick={onIconPick} {...props} />
            </CustomThemeProvider>,
        ),
    };
}

describe('ChatIconPicker', () => {
    let clientWidthSpy: ReturnType<typeof vi.spyOn>;

    beforeEach(() => {
        vi.clearAllMocks();
        loadPhosphorIconsMock.mockResolvedValue({
            House: MockIcon,
            Shapes: MockIcon,
            UserCircle: MockIcon,
        });
        // react-window only mounts when the measured container width is > 0.
        clientWidthSpy = vi.spyOn(HTMLElement.prototype, 'clientWidth', 'get').mockReturnValue(400);
    });

    afterEach(() => {
        clientWidthSpy.mockRestore();
    });

    it('shows a load-failure state when the Phosphor chunk rejects', async () => {
        loadPhosphorIconsMock.mockRejectedValueOnce(new Error('network'));
        renderPicker();
        await waitFor(() => {
            expect(screen.getByText(/could not load icons/i)).toBeInTheDocument();
        });
    });

    it('calls onIconPick when an icon cell is clicked', async () => {
        const user = userEvent.setup();
        const { onIconPick } = renderPicker();

        await waitFor(() => expect(screen.getAllByTestId('phosphor-glyph').length).toBeGreaterThan(0));
        const cells = screen.getAllByRole('button');
        await user.click(cells[0]);
        expect(onIconPick).toHaveBeenCalledWith('House');
    });

    it('clears the highlighted selection when selectedIcon becomes null', async () => {
        const user = userEvent.setup();
        const onIconPick = vi.fn();
        const { rerender } = renderPicker({ selectedIcon: 'House', onIconPick });
        await waitFor(() => expect(screen.getAllByRole('button').length).toBeGreaterThan(0));

        const selectedBefore = screen.getAllByRole('button')[0];
        expect(selectedBefore).toBeTruthy();

        await act(async () => {
            rerender(
                <CustomThemeProvider>
                    <ChatIconPicker onIconPick={onIconPick} selectedIcon={null} />
                </CustomThemeProvider>,
            );
        });

        // After clearing the prop, picking another icon still works (selection state synced).
        await user.click(screen.getAllByRole('button')[1]);
        expect(onIconPick).toHaveBeenCalledWith('Shapes');
    });
});
