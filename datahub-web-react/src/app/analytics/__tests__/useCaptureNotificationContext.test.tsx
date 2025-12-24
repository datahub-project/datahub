import { renderHook } from '@testing-library/react-hooks';
import React from 'react';
import { MemoryRouter } from 'react-router-dom';
import { vi } from 'vitest';

import { NOTIFICATION_CONTEXT_STORAGE_KEY } from '@app/analytics/notificationTracking';
import { useCaptureNotificationContext } from '@app/analytics/useCaptureNotificationContext';

const { mockEvent } = vi.hoisted(() => ({
    mockEvent: vi.fn(),
}));

vi.mock('@app/analytics', () => ({
    default: {
        event: mockEvent,
    },
    EventType: {
        NotificationOpenEvent: 999,
    },
}));

describe('useCaptureNotificationContext', () => {
    beforeEach(() => {
        sessionStorage.clear();
        mockEvent.mockClear();
    });

    it('stores notification context in sessionStorage and emits NotificationOpenEvent once per notification id', () => {
        const wrapper = ({ children }: { children: React.ReactNode }) => (
            <MemoryRouter
                initialEntries={[
                    '/datasets/test?notification_type=incident&notification_id=urn%3Ali%3Aincident%3Atest&notification_channel=slack',
                ]}
            >
                {children}
            </MemoryRouter>
        );

        renderHook(() => useCaptureNotificationContext(), { wrapper });

        const stored = sessionStorage.getItem(NOTIFICATION_CONTEXT_STORAGE_KEY);
        expect(stored).toBeTruthy();
        expect(JSON.parse(stored as string)).toMatchObject({
            type: 'notification',
            notification: {
                type: 'incident',
                notificationId: 'urn:li:incident:test',
                notificationChannel: 'slack',
            },
        });

        expect(mockEvent).toHaveBeenCalledTimes(1);

        // Rendering again with same notification should not emit again (emit key is stored in sessionStorage)
        renderHook(() => useCaptureNotificationContext(), { wrapper });
        expect(mockEvent).toHaveBeenCalledTimes(1);
    });
});
