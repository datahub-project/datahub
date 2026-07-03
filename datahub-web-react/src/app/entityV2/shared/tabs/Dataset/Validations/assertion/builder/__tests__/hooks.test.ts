import { act, renderHook } from '@testing-library/react-hooks';
import { message } from 'antd';
import { useHistory, useLocation } from 'react-router';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import {
    useAssertionURNCopyLink,
    useOpenAssertionDetailModal,
} from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/hooks';
import { getQueryParams } from '@app/entityV2/shared/tabs/Dataset/Validations/assertionUtils';

vi.mock('antd', () => ({
    message: {
        success: vi.fn(),
        error: vi.fn(),
    },
}));

vi.mock('react-router', () => ({
    useLocation: vi.fn(),
    useHistory: vi.fn(),
}));

vi.mock('@app/entityV2/shared/tabs/Dataset/Validations/assertionUtils', () => ({
    getQueryParams: vi.fn(),
}));

// ---------------------------------------------------------------------------
// useAssertionURNCopyLink
// ---------------------------------------------------------------------------

describe('useAssertionURNCopyLink', () => {
    const TEST_URN = 'urn:li:assertion:abc123';
    const mockWriteText = vi.fn();

    beforeEach(() => {
        Object.assign(navigator, { clipboard: { writeText: mockWriteText } });
    });

    afterEach(() => {
        vi.clearAllMocks();
    });

    it('returns a callable function', () => {
        const { result } = renderHook(() => useAssertionURNCopyLink(TEST_URN));
        expect(typeof result.current).toBe('function');
    });

    it('calls clipboard.writeText with a URL that encodes the assertion URN', async () => {
        mockWriteText.mockResolvedValueOnce(undefined);
        const { result } = renderHook(() => useAssertionURNCopyLink(TEST_URN));

        await act(async () => {
            result.current();
        });

        const expectedUrl = new URL(window.location.href);
        expectedUrl.searchParams.set('assertion_urn', encodeURIComponent(TEST_URN));
        expect(mockWriteText).toHaveBeenCalledWith(expectedUrl.href);
    });

    it('shows a success message when the clipboard write succeeds', async () => {
        mockWriteText.mockResolvedValueOnce(undefined);
        const { result } = renderHook(() => useAssertionURNCopyLink(TEST_URN));

        await act(async () => {
            result.current();
        });

        expect(message.success).toHaveBeenCalledWith('Link copied to clipboard!');
        expect(message.error).not.toHaveBeenCalled();
    });

    it('shows an error message when the clipboard write fails', async () => {
        mockWriteText.mockRejectedValueOnce(new Error('clipboard denied'));
        const { result } = renderHook(() => useAssertionURNCopyLink(TEST_URN));

        await act(async () => {
            result.current();
        });

        expect(message.error).toHaveBeenCalledWith('Failed to copy link to clipboard.');
        expect(message.success).not.toHaveBeenCalled();
    });
});

// ---------------------------------------------------------------------------
// useOpenAssertionDetailModal
// ---------------------------------------------------------------------------

describe('useOpenAssertionDetailModal', () => {
    const mockReplace = vi.fn();
    const mockSetFocusUrn = vi.fn();

    const mockLocation = (search: string, pathname = '/dataset/test') => {
        (useLocation as unknown as ReturnType<typeof vi.fn>).mockReturnValue({ search, pathname });
    };

    beforeEach(() => {
        (useHistory as unknown as ReturnType<typeof vi.fn>).mockReturnValue({ replace: mockReplace });
        (getQueryParams as unknown as ReturnType<typeof vi.fn>).mockReturnValue(null);
        mockLocation('');
    });

    afterEach(() => {
        vi.clearAllMocks();
    });

    it('returns the assertionUrnParam from getQueryParams', () => {
        (getQueryParams as unknown as ReturnType<typeof vi.fn>).mockReturnValue('urn%3Ali%3Aassertion%3Atest');
        mockLocation('?assertion_urn=urn%3Ali%3Aassertion%3Atest');

        const { result } = renderHook(() => useOpenAssertionDetailModal(mockSetFocusUrn));

        expect(result.current.assertionUrnParam).toBe('urn%3Ali%3Aassertion%3Atest');
    });

    it('calls setFocusAssertionUrn with the decoded URN when the param is present', () => {
        const encoded = 'urn%3Ali%3Aassertion%3Atest';
        (getQueryParams as unknown as ReturnType<typeof vi.fn>).mockReturnValue(encoded);
        mockLocation(`?assertion_urn=${encoded}`);

        renderHook(() => useOpenAssertionDetailModal(mockSetFocusUrn));

        expect(mockSetFocusUrn).toHaveBeenCalledWith('urn:li:assertion:test');
    });

    it('removes assertion_urn from the URL and calls history.replace', () => {
        const encoded = 'urn%3Ali%3Aassertion%3Atest';
        (getQueryParams as unknown as ReturnType<typeof vi.fn>).mockReturnValue(encoded);
        mockLocation(`?assertion_urn=${encoded}&tab=assertions`, '/dataset/test');

        renderHook(() => useOpenAssertionDetailModal(mockSetFocusUrn));

        expect(mockReplace).toHaveBeenCalledWith('/dataset/test?tab=assertions');
    });

    it('calls history.replace with a trailing "?" when assertion_urn is the only param', () => {
        const encoded = 'urn%3Ali%3Aassertion%3Atest';
        (getQueryParams as unknown as ReturnType<typeof vi.fn>).mockReturnValue(encoded);
        mockLocation(`?assertion_urn=${encoded}`, '/dataset/test');

        renderHook(() => useOpenAssertionDetailModal(mockSetFocusUrn));

        expect(mockReplace).toHaveBeenCalledWith('/dataset/test?');
    });

    it('does not call setFocusAssertionUrn when the param is absent', () => {
        (getQueryParams as unknown as ReturnType<typeof vi.fn>).mockReturnValue(null);
        mockLocation('');

        renderHook(() => useOpenAssertionDetailModal(mockSetFocusUrn));

        expect(mockSetFocusUrn).not.toHaveBeenCalled();
    });

    it('does not call history.replace when the param is absent', () => {
        (getQueryParams as unknown as ReturnType<typeof vi.fn>).mockReturnValue(null);
        mockLocation('');

        renderHook(() => useOpenAssertionDetailModal(mockSetFocusUrn));

        expect(mockReplace).not.toHaveBeenCalled();
    });

    it('returns null assertionUrnParam when the param is absent', () => {
        (getQueryParams as unknown as ReturnType<typeof vi.fn>).mockReturnValue(null);
        mockLocation('');

        const { result } = renderHook(() => useOpenAssertionDetailModal(mockSetFocusUrn));

        expect(result.current.assertionUrnParam).toBeNull();
    });
});
