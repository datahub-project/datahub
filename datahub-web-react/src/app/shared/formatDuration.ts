/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

export const formatDuration = (durationMs: number): string => {
    if (!durationMs) return 'None';

    const seconds = durationMs / 1000;

    if (seconds < 60) {
        return `${seconds.toFixed(1)} s`;
    }

    const minutes = Math.floor(seconds / 60);
    const remainingSeconds = Math.round(seconds % 60);

    if (minutes < 60) {
        return `${minutes} min ${remainingSeconds} s`;
    }

    const hours = Math.floor(minutes / 60);
    const remainingMinutes = Math.round(minutes % 60);

    return `${hours} hr ${remainingMinutes} min`;
};
