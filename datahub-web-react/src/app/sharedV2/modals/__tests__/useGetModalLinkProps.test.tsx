import { renderHook } from '@testing-library/react-hooks';
import React, { PropsWithChildren } from 'react';
import { describe, expect, it } from 'vitest';

import { ModalContext, ModalContextType } from '@app/sharedV2/modals/ModalContext';
import { useGetModalLinkProps } from '@app/sharedV2/modals/useGetModalLinkProps';

function wrapperWithContext(value: ModalContextType) {
    return ({ children }: { children: React.ReactNode }) => (
        <ModalContext.Provider value={value}>{children}</ModalContext.Provider>
    );
}

describe('useGetModalLinkProps', () => {
    it('should return an empty object when not in modal (default context)', () => {
        const { result } = renderHook(() => useGetModalLinkProps());
        expect(result.current).toEqual({});
    });

    it('should return correct props when isInsideModal = true', () => {
        const { result } = renderHook(() => useGetModalLinkProps(), {
            wrapper: wrapperWithContext({ isInsideModal: true }),
        });
        expect(result.current).toEqual({
            target: '_blank',
            rel: 'noopener noreferrer',
        });
    });

    it('should return empty object when isInsideModal = false', () => {
        const { result } = renderHook(() => useGetModalLinkProps(), {
            wrapper: wrapperWithContext({ isInsideModal: false }),
        });
        expect(result.current).toEqual({});
    });

    it('should memoize result for same context value', () => {
        const { result, rerender } = renderHook(() => useGetModalLinkProps(), {
            wrapper: wrapperWithContext({ isInsideModal: true }),
        });
        const first = result.current;
        rerender();
        expect(result.current).toBe(first); // stable reference
    });

    it('should change reference when context value changes', () => {
        const wrapper = ({ children, value }: PropsWithChildren<{ value: boolean }>) => (
            <ModalContext.Provider value={{ isInsideModal: value }}>{children}</ModalContext.Provider>
        );

        const { result, rerender } = renderHook(() => useGetModalLinkProps(), {
            initialProps: { value: false },
            wrapper,
        });

        const first = result.current;
        rerender({ value: true });
        expect(result.current).not.toBe(first);
        expect(result.current).toEqual({
            target: '_blank',
            rel: 'noopener noreferrer',
        });
    });

    it('should have no extra keys when not in modal', () => {
        const { result } = renderHook(() => useGetModalLinkProps(), {
            wrapper: wrapperWithContext({ isInsideModal: false }),
        });
        expect(Object.keys(result.current)).toHaveLength(0);
    });

    it('should match snapshot when in modal', () => {
        const { result } = renderHook(() => useGetModalLinkProps(), {
            wrapper: wrapperWithContext({ isInsideModal: true }),
        });
        expect(result.current).toMatchInlineSnapshot(`
      {
        "rel": "noopener noreferrer",
        "target": "_blank",
      }
    `);
    });
});
