/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { typography } from '@components/theme';

export const getHeaderTitleStyles = (variant) => {
    if (variant === 'sectionHeader') {
        return {
            fontSize: typography.fontSizes.lg,
        };
    }
    return {
        fontSize: typography.fontSizes['3xl'],
    };
};

export const getHeaderSubtitleStyles = (variant) => {
    if (variant === 'sectionHeader') {
        return {
            fontSize: typography.fontSizes.md,
        };
    }
    return {
        fontSize: typography.fontSizes.lg,
    };
};
