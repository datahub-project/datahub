/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import DOMPurify from 'dompurify';

export const sanitizeRichText = (content: string) => {
    if (!content) return '';
    const encoded = content.replace(/</g, '&lt;').replace(/>/g, '&gt;');
    return DOMPurify.sanitize(encoded).replace(/&lt;/g, '<').replace(/&gt;/g, '>');
};

export function ptToPx(pt: number): number {
    return Math.round((pt * 96) / 72);
}

export function pxToPt(px: number): number {
    return Math.round((px * 72) / 96);
}
