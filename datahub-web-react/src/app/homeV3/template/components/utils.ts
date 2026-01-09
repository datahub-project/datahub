import { ClientRect, CollisionDetection } from '@dnd-kit/core';
import { Coordinates } from '@dnd-kit/utilities';

import { PageTemplateSurfaceType } from '@types';

// Distance threshold in pixels - ignore droppables beyond this distance
const DISTANCE_THRESHOLD = 200;

/**
 * Calculate distance between a point and the nearest edge of a rectangle
 * @param point Coordinates of the point
 * @param rect Rectangle boundaries
 * @returns Distance to nearest edge (0 if inside rectangle)
 */
const getDistanceToRect = (point: Coordinates, rect: ClientRect): number => {
    const { x, y } = point;
    const { left, right, top, bottom } = rect;

    // Point is inside rectangle - return distance to nearest edge
    if (x >= left && x <= right && y >= top && y <= bottom) {
        const toLeft = x - left;
        const toRight = right - x;
        const toTop = y - top;
        const toBottom = bottom - y;
        return Math.min(toLeft, toRight, toTop, toBottom);
    }

    // Point is outside rectangle - calculate euclidean distance to nearest corner/edge
    let dx = 0;
    let dy = 0;

    // Calculate horizontal distance
    if (x < left) dx = left - x;
    else if (x > right) dx = x - right;

    // Calculate vertical distance
    if (y < top) dy = top - y;
    else if (y > bottom) dy = y - bottom;

    return Math.sqrt(dx * dx + dy * dy);
};

/**
 * Custom collision detection strategy that finds droppables closest to the pointer
 * Filters out droppables beyond DISTANCE_THRESHOLD and sorts by proximity
 */
export const pointerClosestCollisionDetector: CollisionDetection = ({ droppableRects, pointerCoordinates }) => {
    // Early return if no pointer position available
    if (!pointerCoordinates) return [];

    // Calculate distances for all droppables
    const collisions = Array.from(droppableRects.entries()).map(([id, rect]) => ({
        id,
        data: { distance: getDistanceToRect(pointerCoordinates, rect) },
    }));

    // Filter out distant droppables and sort by proximity
    return collisions
        .filter((collision) => collision.data.distance < DISTANCE_THRESHOLD)
        .sort((a, b) => a.data.distance - b.data.distance);
};

export type BottomAddButtonMode = 'homeWithRows' | 'homeWithoutRows' | 'assetSummary';

export function getBottomButtonMode(templateType: PageTemplateSurfaceType, hasRows: boolean): BottomAddButtonMode {
    if (templateType === PageTemplateSurfaceType.AssetSummary) {
        return 'assetSummary';
    }
    if (hasRows) return 'homeWithRows';
    return 'homeWithoutRows';
}
