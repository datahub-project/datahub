/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import {
    CONTAINER_FILTER_NAME,
    DOMAINS_FILTER_NAME,
    ENTITY_FILTER_NAME,
    GLOSSARY_TERMS_FILTER_NAME,
    ORIGIN_FILTER_NAME,
    OWNERS_FILTER_NAME,
    PLATFORM_FILTER_NAME,
    TAGS_FILTER_NAME,
    TYPE_NAMES_FILTER_NAME,
} from '@app/search/utils/constants';

import { EntityType } from '@types';

export const SORTED_FILTERS = [
    PLATFORM_FILTER_NAME,
    ORIGIN_FILTER_NAME,
    DOMAINS_FILTER_NAME,
    ENTITY_FILTER_NAME,
    TYPE_NAMES_FILTER_NAME,
    GLOSSARY_TERMS_FILTER_NAME,
    OWNERS_FILTER_NAME,
    TAGS_FILTER_NAME,
    CONTAINER_FILTER_NAME,
];

export const FACETS_TO_ENTITY_TYPES = {
    [DOMAINS_FILTER_NAME]: [EntityType.Domain],
    [GLOSSARY_TERMS_FILTER_NAME]: [EntityType.GlossaryTerm],
    [OWNERS_FILTER_NAME]: [EntityType.CorpUser, EntityType.CorpGroup],
    [TAGS_FILTER_NAME]: [EntityType.Tag],
    [CONTAINER_FILTER_NAME]: [EntityType.Container],
};
