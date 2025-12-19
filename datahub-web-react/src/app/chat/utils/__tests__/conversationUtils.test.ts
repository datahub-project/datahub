import { vi } from 'vitest';

import { mapToConversationListItem } from '@app/chat/utils/conversationUtils';

describe('conversationUtils', () => {
    beforeEach(() => {
        vi.useFakeTimers();
        vi.setSystemTime(new Date('2024-01-01T00:00:00Z'));
    });

    afterEach(() => {
        vi.useRealTimers();
    });

    it('maps conversation fields with defaults', () => {
        const result = mapToConversationListItem({ urn: 'urn1', title: 'Hi' });
        expect(result).toEqual({
            urn: 'urn1',
            title: 'Hi',
            messageCount: 0,
            created: { time: new Date('2024-01-01T00:00:00Z').getTime() },
            lastUpdated: { time: new Date('2024-01-01T00:00:00Z').getTime() },
        });
    });

    it('preserves provided fields', () => {
        const result = mapToConversationListItem({
            urn: 'urn2',
            title: null,
            messageCount: 3,
            created: { time: 1 },
            lastUpdated: { time: 2 },
        });
        expect(result).toEqual({
            urn: 'urn2',
            title: null,
            messageCount: 3,
            created: { time: 1 },
            lastUpdated: { time: 2 },
        });
    });
});
