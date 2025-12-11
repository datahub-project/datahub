/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

/**
 * Compute top offset to render a few "whiskers" on on chart
 */
export function computeWhiskerOffset(
    numberOfAllWhiskers: number,
    numberOfCurrentWhisker: number,
    whiskerBoxSize: number,
    allWidth: number,
    gapBetweenWhiskers: number,
): number {
    const numRectangles = numberOfAllWhiskers;

    // Calculate the total height occupied by rectangles and gaps
    const totalWidthRequired = numRectangles * whiskerBoxSize + (numRectangles - 1) * gapBetweenWhiskers;

    // Calculate the starting Y position to center the group of rectangles vertically
    const start = (allWidth - totalWidthRequired) / 2;

    const offset = start + numberOfCurrentWhisker * (whiskerBoxSize + gapBetweenWhiskers);
    return offset;
}
