/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

export const getLighterRGBColor = (red, green, blue) => {
    const lighterRed = red + 0.9 * (255 - red);
    const lighterGreen = green + 0.9 * (255 - green);
    const lighterBlue = blue + 0.9 * (255 - blue);

    return [lighterRed, lighterGreen, lighterBlue];
};
