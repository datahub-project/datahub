/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

// Helper function to validate image URLs
export const isValidImageUrl = async (url: string): Promise<boolean> => {
    return new Promise((resolve) => {
        const img = new Image();
        img.src = url;

        img.onload = () => resolve(true); // Image is valid
        img.onerror = () => resolve(false); // Image is invalid
    });
};
