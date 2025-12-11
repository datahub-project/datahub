/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

export const getGreetingText = () => {
    const currentHour = new Date().getHours(); // gets the current hour (0-23)
    if (currentHour < 12) {
        return 'Good morning';
    }
    if (currentHour < 17) {
        return 'Good afternoon';
    }
    return 'Good evening';
};
