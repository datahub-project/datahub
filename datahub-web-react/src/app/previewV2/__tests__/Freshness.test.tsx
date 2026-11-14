import { MockedProvider } from '@apollo/client/testing';
import { cleanup, fireEvent, render, screen } from '@testing-library/react';
import React from 'react';

import Freshness from '@app/previewV2/Freshness';
import TestPageContainer from '@utils/test-utils/TestPageContainer';
import { AGE_COLOR, NEUTRAL, SIXTY_DAYS_AGO, clockColor } from '@utils/test-utils/freshnessClock';

function renderClock(neutral?: boolean) {
    render(
        <MockedProvider mocks={[]}>
            <TestPageContainer>
                <Freshness time={SIXTY_DAYS_AGO} timeProperty="lastModified" showDate={false} neutral={neutral} />
            </TestPageContainer>
        </MockedProvider>,
    );
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
        renderClock();
        expect(clockColor()).toBe(AGE_COLOR);
    });

    it('shows an old definition time in the neutral icon color', () => {
        renderClock(true);
        expect(clockColor()).toBe(NEUTRAL);
    });

    it('keeps the popover text when the color is neutral', async () => {
        const text = await popoverText();
        expect(text).toMatch(/2 months/);
        expect(await popoverText(true)).toBe(text);
    });
});
