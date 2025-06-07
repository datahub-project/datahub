import { renderHook } from '@testing-library/react-hooks';

import useCommandS from '@app/ingestV2/shared/hooks/useCommandS';
import { checkIfMac } from '@app/utils/checkIfMac';

// Mock the checkIfMac utility
vi.mock('@app/utils/checkIfMac', () => ({
    checkIfMac: vi.fn(),
}));

describe('useCommandS Hook', () => {
    const mockOnPress = vi.fn();

    beforeEach(() => {
        vi.clearAllMocks();
    });

    it('adds and removes event listener on mount/unmount', () => {
        const addSpy = vi.spyOn(window, 'addEventListener');
        const removeSpy = vi.spyOn(window, 'removeEventListener');

        const { unmount } = renderHook(() => useCommandS(mockOnPress));

        expect(addSpy).toHaveBeenCalledWith('keydown', expect.any(Function));
        unmount();
        expect(removeSpy).toHaveBeenCalledWith('keydown', expect.any(Function));
    });

    it('triggers onPress with Command+S on Mac', () => {
        vi.mocked(checkIfMac).mockReturnValue(true);
        renderHook(() => useCommandS(mockOnPress));

        const event = new KeyboardEvent('keydown', {
            key: 's',
            metaKey: true,
            ctrlKey: false,
            bubbles: true,
            cancelable: true,
        });

        // Dispatch event and verify onPress is called
        const preventDefault = vi.fn();
        event.preventDefault = preventDefault;
        window.dispatchEvent(event);

        expect(mockOnPress).toHaveBeenCalled();
        expect(preventDefault).toHaveBeenCalled();
    });

    it('triggers onPress with Ctrl+S on non-Mac', () => {
        vi.mocked(checkIfMac).mockReturnValue(false);
        renderHook(() => useCommandS(mockOnPress));

        const event = new KeyboardEvent('keydown', {
            key: 's',
            metaKey: false,
            ctrlKey: true,
            bubbles: true,
            cancelable: true,
        });

        const preventDefault = vi.fn();
        event.preventDefault = preventDefault;
        window.dispatchEvent(event);

        expect(mockOnPress).toHaveBeenCalled();
        expect(preventDefault).toHaveBeenCalled();
    });

    it('does NOT trigger onPress with incorrect key', () => {
        vi.mocked(checkIfMac).mockReturnValue(true);
        renderHook(() => useCommandS(mockOnPress));

        const event = new KeyboardEvent('keydown', {
            key: 'k',
            metaKey: true,
            ctrlKey: false,
        });

        window.dispatchEvent(event);
        expect(mockOnPress).not.toHaveBeenCalled();
    });
});
