import { DragEndEvent } from '@dnd-kit/core';
import { act, renderHook } from '@testing-library/react-hooks';
import { describe, expect, it, vi } from 'vitest';

import { useVerticalDragAndDrop } from '@app/homeV3/modules/assetCollection/dragAndDrop/useVerticalDragAndDrop';

describe('useVerticalDragAndDrop', () => {
    const initialItems = ['a', 'b', 'c', 'd'];

    it('should return correct sensors, strategy, collisionDetection, and modifiers', () => {
        const { result } = renderHook(() => useVerticalDragAndDrop({ items: initialItems, onChange: vi.fn() }));
        const { sensors, strategy, collisionDetection, modifiers } = result.current;

        expect(sensors).toBeDefined();
        expect(strategy).toBeDefined();
        expect(collisionDetection).toBeDefined();
        expect(Array.isArray(modifiers)).toBe(true);
        expect(modifiers.length).toBeGreaterThan(0);
    });

    describe('handleDragEnd callback behavior', () => {
        it('should do nothing if over is null', () => {
            const onChange = vi.fn();
            const { result } = renderHook(() => useVerticalDragAndDrop({ items: initialItems, onChange }));

            const event = { active: { id: 'a' }, over: null } as unknown as DragEndEvent;
            act(() => {
                result.current.handleDragEnd(event);
            });

            expect(onChange).not.toHaveBeenCalled();
        });

        it('should do nothing if active.id is equal to over.id', () => {
            const onChange = vi.fn();
            const { result } = renderHook(() => useVerticalDragAndDrop({ items: initialItems, onChange }));

            const event = { active: { id: 'b' }, over: { id: 'b' } } as unknown as DragEndEvent;
            act(() => {
                result.current.handleDragEnd(event);
            });

            expect(onChange).not.toHaveBeenCalled();
        });

        it('should call onChange with new order if active and over ids are different and in items', () => {
            const onChange = vi.fn();
            // items in order: ['a','b','c','d']
            const { result } = renderHook(() => useVerticalDragAndDrop({ items: initialItems, onChange }));

            // simulate dragging 'a' over 'c'
            const event = { active: { id: 'a' }, over: { id: 'c' } } as unknown as DragEndEvent;
            act(() => {
                result.current.handleDragEnd(event);
            });

            // Expected new order: ['b', 'c', 'a', 'd']
            expect(onChange).toHaveBeenCalledTimes(1);
            expect(onChange).toHaveBeenCalledWith(['b', 'c', 'a', 'd']);
        });

        it('should do nothing if active.id or over.id not in items', () => {
            const onChange = vi.fn();
            const { result } = renderHook(() => useVerticalDragAndDrop({ items: initialItems, onChange }));

            // active.id not in items
            let event = { active: { id: 'x' }, over: { id: 'a' } } as unknown as DragEndEvent;
            act(() => {
                result.current.handleDragEnd(event);
            });
            expect(onChange).not.toHaveBeenCalled();

            // over.id not in items
            event = { active: { id: 'a' }, over: { id: 'x' } } as unknown as DragEndEvent;
            act(() => {
                result.current.handleDragEnd(event);
            });
            expect(onChange).not.toHaveBeenCalled();
        });
    });

    it('should update handleDragEnd callback when items or onChange changes', () => {
        const onChange1 = vi.fn();
        const onChange2 = vi.fn();
        const { result, rerender } = renderHook(({ items, onChange }) => useVerticalDragAndDrop({ items, onChange }), {
            initialProps: { items: initialItems, onChange: onChange1 },
        });

        const firstHandleDragEnd = result.current.handleDragEnd;

        // Re-render with new items
        rerender({ items: ['a', 'c', 'b', 'd'], onChange: onChange1 });
        expect(result.current.handleDragEnd).not.toBe(firstHandleDragEnd);

        // Re-render with new onChange callback
        rerender({ items: initialItems, onChange: onChange2 });
        expect(result.current.handleDragEnd).not.toBe(firstHandleDragEnd);
    });
});
