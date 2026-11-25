/**
 * How many pills, in order, fit on one row of `available` pixels. When some don't fit, room is
 * kept for the "+N" pill that stands in for the rest.
 */
export function countFittingPills(pillWidths: number[], available: number, gap: number, moreWidth: number): number {
    const rowWidth = (count: number) =>
        pillWidths.slice(0, count).reduce((sum, width) => sum + width, 0) + gap * Math.max(count - 1, 0);
    if (rowWidth(pillWidths.length) <= available) return pillWidths.length;
    let count = pillWidths.length - 1;
    while (count > 0 && rowWidth(count) + gap + moreWidth > available) count -= 1;
    return count;
}
