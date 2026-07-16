import type { ClientRect } from '@dnd-kit/core';

import { getBottomButtonMode, pointerClosestCollisionDetector } from '@app/homeV3/template/components/utils';

import { PageTemplateSurfaceType } from '@types';

// Helper to create rectangles
const createRect = (x: number, y: number, width: number, height: number): ClientRect => ({
    left: x,
    top: y,
    right: x + width,
    bottom: y + height,
    width,
    height,
});

describe('pointerClosestCollisionDetector', () => {
    it('returns empty array when pointerCoordinates is null', () => {
        const droppableRects = new Map([['box1', createRect(0, 0, 100, 100)]]);
        const result = pointerClosestCollisionDetector({
            droppableRects,
            pointerCoordinates: null,
        } as any);

        expect(result).toEqual([]);
    });

    it('returns empty array when there are no droppable areas', () => {
        const result = pointerClosestCollisionDetector({
            droppableRects: new Map(),
            pointerCoordinates: { x: 50, y: 50 },
        } as any);

        expect(result).toEqual([]);
    });

    it('includes droppable when pointer is inside it', () => {
        const rect = createRect(0, 0, 100, 100);
        const pointer = { x: 10, y: 10 };

        const droppableRects = new Map([['box1', rect]]);
        const result = pointerClosestCollisionDetector({
            droppableRects,
            pointerCoordinates: pointer,
        } as any);

        expect(result.map((c) => c.id)).toEqual(['box1']);
    });

    it('excludes droppables beyond threshold distance', () => {
        const closeRect = createRect(0, 0, 100, 100);
        const farRect = createRect(300, 300, 100, 100);

        const droppableRects = new Map([
            ['close', closeRect],
            ['far', farRect],
        ]);

        const pointer = { x: 50, y: 50 };
        const result = pointerClosestCollisionDetector({
            droppableRects,
            pointerCoordinates: pointer,
        } as any);

        expect(result.map((c) => c.id)).toEqual(['close']);
    });

    it('sorts droppables by proximity to pointer', () => {
        const closestRect = createRect(95, 95, 10, 10);
        const nearRect = createRect(50, 50, 40, 40);
        const midRect = createRect(150, 150, 50, 50);

        const droppableRects = new Map([
            ['mid', midRect],
            ['near', nearRect],
            ['closest', closestRect],
        ]);

        const pointer = { x: 100, y: 100 };
        const result = pointerClosestCollisionDetector({
            droppableRects,
            pointerCoordinates: pointer,
        } as any);

        // Verify order of IDs reflects proximity
        expect(result.map((c) => c.id)).toEqual(['closest', 'near', 'mid']);
    });

    it('handles multiple rectangles at same distance', () => {
        const rect1 = createRect(150, 0, 50, 50); // 100px from pointer
        const rect2 = createRect(0, 150, 50, 50); // 100px from pointer
        const rect3 = createRect(0, 0, 100, 100); // 50px from pointer

        const droppableRects = new Map([
            ['rect1', rect1],
            ['rect2', rect2],
            ['rect3', rect3],
        ]);

        const pointer = { x: 150, y: 150 };
        const result = pointerClosestCollisionDetector({
            droppableRects,
            pointerCoordinates: pointer,
        } as any);

        // rect3 should be first since it's closest
        expect(result[0].id).toBe('rect3');

        // The two equally distant items should both be present
        expect(result.map((c) => c.id)).toEqual(expect.arrayContaining(['rect1', 'rect2']));
    });

    it('includes all droppables within threshold in proximity order', () => {
        const rect1 = createRect(0, 0, 100, 100); // ~50px from pointer
        const rect2 = createRect(200, 200, 100, 100); // ~141px from pointer
        const rect3 = createRect(300, 300, 100, 100); // >200px (excluded)

        const droppableRects = new Map([
            ['rect1', rect1],
            ['rect2', rect2],
            ['rect3', rect3],
        ]);

        const pointer = { x: 150, y: 150 };
        const result = pointerClosestCollisionDetector({
            droppableRects,
            pointerCoordinates: pointer,
        } as any);

        // Verify included items and order
        expect(result.map((c) => c.id)).toEqual(['rect1', 'rect2']);
    });

    it('handles pointer at exact rectangle boundaries', () => {
        const rect = createRect(0, 0, 100, 100);
        const droppableRects = new Map([['boundary', rect]]);

        // Test all four corners
        const corners = [
            { x: 0, y: 0 }, // top-left
            { x: 100, y: 0 }, // top-right
            { x: 0, y: 100 }, // bottom-left
            { x: 100, y: 100 }, // bottom-right
        ];

        corners.forEach((pointer) => {
            const result = pointerClosestCollisionDetector({
                droppableRects,
                pointerCoordinates: pointer,
            } as any);

            expect(result.map((c) => c.id)).toEqual(['boundary']);
            expect(result[0]?.data?.distance).toBe(0); // Should be 0 since pointer is on boundary
        });
    });

    it('handles zero-sized rectangles', () => {
        const zeroRect = createRect(50, 50, 0, 0);
        const droppableRects = new Map([['zero', zeroRect]]);
        const pointer = { x: 50, y: 50 };

        const result = pointerClosestCollisionDetector({
            droppableRects,
            pointerCoordinates: pointer,
        } as any);

        expect(result.map((c) => c.id)).toEqual(['zero']);
        expect(result?.[0]?.data?.distance).toBe(0);
    });

    it('handles negative coordinates', () => {
        const negativeRect = createRect(-100, -100, 50, 50);
        const droppableRects = new Map([['negative', negativeRect]]);
        const pointer = { x: -75, y: -75 };

        const result = pointerClosestCollisionDetector({
            droppableRects,
            pointerCoordinates: pointer,
        } as any);

        expect(result.map((c) => c.id)).toEqual(['negative']);
        expect(result?.[0]?.data?.distance).toBe(25); // Distance to nearest edge
    });

    it('filters out all rectangles when all are beyond threshold', () => {
        const farRect1 = createRect(300, 300, 100, 100);
        const farRect2 = createRect(400, 400, 100, 100);
        const droppableRects = new Map([
            ['far1', farRect1],
            ['far2', farRect2],
        ]);
        const pointer = { x: 0, y: 0 };

        const result = pointerClosestCollisionDetector({
            droppableRects,
            pointerCoordinates: pointer,
        } as any);

        expect(result).toEqual([]);
    });
});

describe('getBottomButtonMode', () => {
    describe('AssetSummary template type', () => {
        it('returns assetSummary regardless of hasRows value', () => {
            expect(getBottomButtonMode(PageTemplateSurfaceType.AssetSummary, true)).toBe('assetSummary');
            expect(getBottomButtonMode(PageTemplateSurfaceType.AssetSummary, false)).toBe('assetSummary');
        });
    });

    describe('HomePage template type', () => {
        it('returns homeWithRows when hasRows is true', () => {
            expect(getBottomButtonMode(PageTemplateSurfaceType.HomePage, true)).toBe('homeWithRows');
        });

        it('returns homeWithoutRows when hasRows is false', () => {
            expect(getBottomButtonMode(PageTemplateSurfaceType.HomePage, false)).toBe('homeWithoutRows');
        });
    });

    describe('type safety and edge cases', () => {
        it('handles boolean edge cases correctly', () => {
            // Test with explicit boolean values
            expect(getBottomButtonMode(PageTemplateSurfaceType.HomePage, Boolean(1))).toBe('homeWithRows');
            expect(getBottomButtonMode(PageTemplateSurfaceType.HomePage, Boolean(0))).toBe('homeWithoutRows');
            expect(getBottomButtonMode(PageTemplateSurfaceType.HomePage, Boolean(''))).toBe('homeWithoutRows');
            expect(getBottomButtonMode(PageTemplateSurfaceType.HomePage, Boolean('any string'))).toBe('homeWithRows');
        });

        it('prioritizes AssetSummary over hasRows parameter', () => {
            // Even with hasRows=true, AssetSummary should return 'assetSummary'
            expect(getBottomButtonMode(PageTemplateSurfaceType.AssetSummary, true)).toBe('assetSummary');
            // Even with hasRows=false, AssetSummary should return 'assetSummary'
            expect(getBottomButtonMode(PageTemplateSurfaceType.AssetSummary, false)).toBe('assetSummary');
        });
    });
});
