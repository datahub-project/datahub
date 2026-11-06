import { MockedProvider } from '@apollo/client/testing';
import { cleanup, fireEvent, render, screen } from '@testing-library/react';
import React from 'react';

import Freshness from '@app/previewV2/Freshness';
import { hexToRgb } from '@app/sharedV2/colors/colorUtils';
import lightTheme from '@conf/theme/colorThemes/light';
import TestPageContainer from '@utils/test-utils/TestPageContainer';

const SIXTY_DAYS_AGO = Date.now() - 60 * 24 * 60 * 60 * 1000;
const AGE_RED = 'rgb(245, 34, 45)';
const NEUTRAL = `rgb(${hexToRgb(lightTheme.icon).join(', ')})`;

function renderClock(neutral?: boolean) {
    render(
        <MockedProvider mocks={[]}>
            <TestPageContainer>
                <Freshness time={SIXTY_DAYS_AGO} timeProperty="lastModified" showDate={false} neutral={neutral} />
            </TestPageContainer>
        </MockedProvider>,
    );
}

function clockColor(neutral?: boolean) {
    renderClock(neutral);
    const icon = screen.getByTestId('freshness').querySelector('svg');
    return icon && getComputedStyle(icon).color;
}

async function popoverText(neutral?: boolean) {
    renderClock(neutral);
    fireEvent.mouseOver(screen.getByTestId('freshness'));
    const text = (await screen.findByRole('dialog')).textContent;
    cleanup();
    return text;
}

describe('Freshness', () => {
    it('colors an old time by its age', () => {
        expect(clockColor()).toBe(AGE_RED);
    });

    it('shows an old definition time in the neutral icon color', () => {
        expect(clockColor(true)).toBe(NEUTRAL);
    });

    it('keeps the popover text when the color is neutral', async () => {
        const text = await popoverText();
        expect(text).toMatch(/2 months/);
        expect(await popoverText(true)).toBe(text);
    });
});
