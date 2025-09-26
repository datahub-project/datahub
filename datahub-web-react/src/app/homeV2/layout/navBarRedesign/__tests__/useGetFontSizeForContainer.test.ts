import { renderHook } from '@testing-library/react-hooks';

import { useGetFontSizeForContainer } from '@app/homeV2/layout/navBarRedesign/useGetFontSizeForContainer';

// Mock DOM methods
Object.defineProperty(HTMLElement.prototype, 'offsetWidth', {
    configurable: true,
    value: 100, // Default container width
});

const mockAppendChild = vi.spyOn(document.body, 'appendChild');
const mockRemoveChild = vi.spyOn(document.body, 'removeChild');

describe('useGetFontSizeForContainer', () => {
    beforeEach(() => {
        mockAppendChild.mockImplementation(() => undefined as any);
        mockRemoveChild.mockImplementation(() => undefined as any);
    });

    afterEach(() => {
        vi.clearAllMocks();
    });

    it('returns base font size initially', () => {
        const { result } = renderHook(() => useGetFontSizeForContainer('Test'));

        expect(result.current.fontSize).toBe(16);
    });

    it('provides container ref that can be used for DOM measurements', () => {
        const { result } = renderHook(() => useGetFontSizeForContainer('Test'));

        expect(result.current.containerRef.current).toBeNull();
        expect(typeof result.current.containerRef).toBe('object');
    });
});
