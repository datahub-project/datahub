/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

export const EMBED_LOOKUP_NOT_FOUND_REASON = {
    NO_ENTITY_FOUND: 'NO_ENTITY_FOUND',
    MULTIPLE_ENTITIES_FOUND: 'MULTIPLE_ENTITIES_FOUND',
} as const;

export type EmbedLookupNotFoundReason =
    (typeof EMBED_LOOKUP_NOT_FOUND_REASON)[keyof typeof EMBED_LOOKUP_NOT_FOUND_REASON];
