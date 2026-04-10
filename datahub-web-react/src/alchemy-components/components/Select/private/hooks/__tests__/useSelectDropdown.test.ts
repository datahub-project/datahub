import { act, renderHook } from '@testing-library/react-hooks';

import useSelectDropdown from '@components/components/Select/private/hooks/useSelectDropdown';

vi.mock('@components/components/Utils/ClickOutside/useClickOutside', () => ({
    default: vi.fn(),
}));

vi.mock('@components/components/Select/private/hooks/useIsVisible', () => ({
    useIsVisible: vi.fn(() => true),
}));

describe('useSelectDropdown', () => {
    const mockSelectRef = { current: document.createElement('div') };
    const mockDropdownRef = { current: document.createElement('div') };

    beforeEach(() => {
        vi.clearAllMocks();
    });

    describe('initialization', () => {
        it('should initialize with defaultOpen state', () => {
            const { result } = renderHook(() => useSelectDropdown(true, mockSelectRef, mockDropdownRef));

            expect(result.current.isOpen).toBe(true);
        });

        it('should initialize with closed state when defaultOpen is false', () => {
            const { result } = renderHook(() => useSelectDropdown(false, mockSelectRef, mockDropdownRef));

            expect(result.current.isOpen).toBe(false);
        });
    });

    describe('open', () => {
        it('should open dropdown and call onOpen callback', () => {
            const mockOnOpen = vi.fn();
            const { result } = renderHook(() =>
                useSelectDropdown(false, mockSelectRef, mockDropdownRef, [], undefined, mockOnOpen),
            );

            act(() => {
                result.current.open();
            });

            expect(result.current.isOpen).toBe(true);
            expect(mockOnOpen).toHaveBeenCalledTimes(1);
        });

        it('should not call onOpen when already open', () => {
            const mockOnOpen = vi.fn();
            const { result } = renderHook(() =>
                useSelectDropdown(true, mockSelectRef, mockDropdownRef, [], undefined, mockOnOpen),
            );

            act(() => {
                result.current.open();
            });

            expect(mockOnOpen).not.toHaveBeenCalled();
        });
    });

    describe('close', () => {
        it('should close dropdown and call onClose callback', () => {
            const mockOnClose = vi.fn();
            const { result } = renderHook(() =>
                useSelectDropdown(true, mockSelectRef, mockDropdownRef, [], mockOnClose),
            );

            act(() => {
                result.current.close();
            });

            expect(result.current.isOpen).toBe(false);
            expect(mockOnClose).toHaveBeenCalledTimes(1);
        });

        it('should not call onClose when already closed', () => {
            const mockOnClose = vi.fn();
            const { result } = renderHook(() =>
                useSelectDropdown(false, mockSelectRef, mockDropdownRef, [], mockOnClose),
            );

            act(() => {
                result.current.close();
            });

            expect(mockOnClose).not.toHaveBeenCalled();
        });
    });

    describe('toggle', () => {
        it('should open when closed', () => {
            const { result } = renderHook(() => useSelectDropdown(false, mockSelectRef, mockDropdownRef));

            act(() => {
                result.current.toggle();
            });

            expect(result.current.isOpen).toBe(true);
        });

        it('should close when open', () => {
            const { result } = renderHook(() => useSelectDropdown(true, mockSelectRef, mockDropdownRef));

            act(() => {
                result.current.toggle();
            });

            expect(result.current.isOpen).toBe(false);
        });

        it('should call onOpen when toggling from closed to open', () => {
            const mockOnOpen = vi.fn();
            const mockOnClose = vi.fn();
            const { result } = renderHook(() =>
                useSelectDropdown(false, mockSelectRef, mockDropdownRef, [], mockOnClose, mockOnOpen),
            );

            act(() => {
                result.current.toggle();
            });

            expect(mockOnOpen).toHaveBeenCalledTimes(1);
            expect(mockOnClose).not.toHaveBeenCalled();
        });

        it('should call onClose when toggling from open to closed', () => {
            const mockOnOpen = vi.fn();
            const mockOnClose = vi.fn();
            const { result } = renderHook(() =>
                useSelectDropdown(true, mockSelectRef, mockDropdownRef, [], mockOnClose, mockOnOpen),
            );

            act(() => {
                result.current.toggle();
            });

            expect(mockOnClose).toHaveBeenCalledTimes(1);
            expect(mockOnOpen).not.toHaveBeenCalled();
        });
    });
});
