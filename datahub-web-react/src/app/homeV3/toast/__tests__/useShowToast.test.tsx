import { act, renderHook } from '@testing-library/react-hooks';
import { notification } from 'antd';
import React from 'react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import useShowToast from '@app/homeV3/toast/useShowToast';

interface NotificationArgsProps {
    message?: React.ReactNode;
    description?: React.ReactNode;
    placement?: string;
    duration?: number;
    icon?: React.ReactNode;
    closeIcon?: React.ReactNode;
    style?: React.CSSProperties;
}

describe('useShowToast', () => {
    let notificationOpenSpy: ReturnType<typeof vi.spyOn>;

    beforeEach(() => {
        notificationOpenSpy = vi.spyOn(notification, 'open').mockImplementation(() => {});
    });

    afterEach(() => {
        vi.clearAllMocks();
    });

    it('should return a showToast function', () => {
        const { result } = renderHook(() => useShowToast());
        expect(typeof result.current.showToast).toBe('function');
    });

    it('should call notification.open with correct config on showToast call', () => {
        const { result } = renderHook(() => useShowToast());

        const sampleTitle = 'Test Title';
        const sampleDescription = 'Sample description text';

        act(() => {
            result.current.showToast(sampleTitle, sampleDescription);
        });

        expect(notificationOpenSpy).toHaveBeenCalledTimes(1);

        const config = notificationOpenSpy.mock.calls[0][0] as NotificationArgsProps;

        expect(config.placement).toBe('bottomRight');
        expect(config.duration).toBe(0);

        expect(config.style).toMatchObject({
            backgroundColor: expect.any(String),
            borderRadius: expect.any(Number),
            width: expect.any(Number),
            padding: expect.any(String),
            right: expect.any(Number),
            bottom: expect.any(Number),
        });

        expect(React.isValidElement(config.icon)).toBe(true);
        expect(React.isValidElement(config.closeIcon)).toBe(true);

        expect(React.isValidElement(config.message)).toBe(true);
        expect(React.isValidElement(config.description)).toBe(true);

        if (React.isValidElement(config.message)) {
            const messageProps = config.message.props;
            expect(messageProps.children).toBe(sampleTitle);
            expect(messageProps.color).toBe('blue');
            expect(messageProps.colorLevel).toBe(1000);
            expect(messageProps.weight).toBe('semiBold');
            expect(messageProps.lineHeight).toBe('sm');
        } else {
            throw new Error('config.message is not a valid React element');
        }

        if (React.isValidElement(config.description)) {
            const descriptionProps = config.description.props;
            expect(descriptionProps.children).toBe(sampleDescription);
            expect(descriptionProps.color).toBe('blue');
            expect(descriptionProps.colorLevel).toBe(1000);
            expect(descriptionProps.lineHeight).toBe('sm');
        } else {
            throw new Error('config.description is not a valid React element');
        }
    });

    it('should handle missing description gracefully', () => {
        const { result } = renderHook(() => useShowToast());

        const sampleTitle = 'Only Title';

        act(() => {
            result.current.showToast(sampleTitle);
        });

        expect(notificationOpenSpy).toHaveBeenCalledTimes(1);

        const config = notificationOpenSpy.mock.calls[0][0] as NotificationArgsProps;

        expect(React.isValidElement(config.message)).toBe(true);

        if (config.description === undefined || config.description === null) {
            expect(config.description).toBeUndefined();
        } else if (React.isValidElement(config.description)) {
            const descriptionProps = config.description.props;
            expect(descriptionProps.children).toBeUndefined();
        } else {
            throw new Error('config.description is not a valid React element or undefined');
        }
    });
});
