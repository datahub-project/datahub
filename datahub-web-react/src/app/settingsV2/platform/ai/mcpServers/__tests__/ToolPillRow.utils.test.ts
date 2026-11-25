import { countFittingPills } from '@app/settingsV2/platform/ai/mcpServers/ToolPillRow.utils';

describe('countFittingPills', () => {
    it('shows every pill when they all fit, without room for a +N pill', () => {
        // 3 x 50 + 2 gaps of 4 = 158.
        expect(countFittingPills([50, 50, 50], 158, 4, 30)).toBe(3);
    });

    it('keeps room for the +N pill when some pills overflow', () => {
        // 2 pills + gap + "+N" = 50 + 4 + 50 + 4 + 30 = 138 fits in 150; a third pill doesn't.
        expect(countFittingPills([50, 50, 50, 50], 150, 4, 30)).toBe(2);
    });

    it('shows only the +N pill when not even one pill fits beside it', () => {
        expect(countFittingPills([120, 50], 100, 4, 30)).toBe(0);
    });
});
