import { BookmarkSimple } from '@phosphor-icons/react/dist/csr/BookmarkSimple';
import { Database } from '@phosphor-icons/react/dist/csr/Database';
import { File } from '@phosphor-icons/react/dist/csr/File';
import { FileText } from '@phosphor-icons/react/dist/csr/FileText';
import { Folder } from '@phosphor-icons/react/dist/csr/Folder';
import { Globe } from '@phosphor-icons/react/dist/csr/Globe';
import { Layout } from '@phosphor-icons/react/dist/csr/Layout';
import { MapPin } from '@phosphor-icons/react/dist/csr/MapPin';
import { Table as TableIcon } from '@phosphor-icons/react/dist/csr/Table';
import { Tag } from '@phosphor-icons/react/dist/csr/Tag';
import { Timer } from '@phosphor-icons/react/dist/csr/Timer';
import { Trash } from '@phosphor-icons/react/dist/csr/Trash';
import { User } from '@phosphor-icons/react/dist/csr/User';
import { Warning } from '@phosphor-icons/react/dist/csr/Warning';
import { Wrench } from '@phosphor-icons/react/dist/csr/Wrench';
import i18next from 'i18next';
import React from 'react';

import { FieldType, FilterField } from '@app/searchV2/filters/types';
import {
    BROWSE_PATH_V2_FILTER_NAME,
    CONTAINER_FILTER_NAME,
    DATA_PLATFORM_INSTANCE_FILTER_NAME,
    DATA_PRODUCT_FILTER_NAME,
    DESCRIPTION_FILTER_NAME,
    DOMAINS_FILTER_NAME,
    ENTITY_FILTER_NAME,
    ENTITY_SUB_TYPE_FILTER_NAME,
    FIELD_DESCRIPTIONS_FILTER_NAME,
    FIELD_GLOSSARY_TERMS_FILTER_NAME,
    FIELD_PATHS_FILTER_NAME,
    FIELD_TAGS_FILTER_NAME,
    FIELD_TO_LABEL,
    GLOSSARY_TERMS_FILTER_NAME,
    HAS_ACTIVE_INCIDENTS_FILTER_NAME,
    HAS_SIBLINGS_FILTER_NAME,
    LAST_MODIFIED_FILTER_NAME,
    ORIGIN_FILTER_NAME,
    OWNERS_FILTER_NAME,
    PARENT_DOCUMENT_FILTER_NAME,
    PLATFORM_FILTER_NAME,
    REMOVED_FILTER_NAME,
    STRUCTURED_PROPERTIES_FILTER_NAME,
    TAGS_FILTER_NAME,
    TYPE_NAMES_FILTER_NAME,
} from '@app/searchV2/utils/constants';

import { EntityType } from '@types';

const ENTITY_SUB_TYPE_FILTER: FilterField = {
    field: ENTITY_SUB_TYPE_FILTER_NAME,
    get displayName() {
        return FIELD_TO_LABEL[ENTITY_SUB_TYPE_FILTER_NAME];
    },
    type: FieldType.NESTED_ENTITY_TYPE,
    icon: <File />,
};

const ENTITY_TYPE_FILTER: FilterField = {
    field: ENTITY_FILTER_NAME,
    get displayName() {
        return FIELD_TO_LABEL[ENTITY_FILTER_NAME];
    },
    type: FieldType.ENTITY_TYPE,
    icon: <File />,
};

const TYPE_NAMES_FILTER: FilterField = {
    field: TYPE_NAMES_FILTER_NAME,
    get displayName() {
        return FIELD_TO_LABEL[TYPE_NAMES_FILTER_NAME];
    },
    type: FieldType.ENUM,
    icon: <File />,
};

const PLATFORM_FILTER: FilterField = {
    field: PLATFORM_FILTER_NAME,
    get displayName() {
        return FIELD_TO_LABEL[PLATFORM_FILTER_NAME];
    },
    type: FieldType.ENTITY,
    entityTypes: [EntityType.DataPlatform],
    icon: <Database />,
};

const OWNERS_FILTER: FilterField = {
    field: OWNERS_FILTER_NAME,
    get displayName() {
        return FIELD_TO_LABEL[OWNERS_FILTER_NAME];
    },
    type: FieldType.ENTITY,
    entityTypes: [EntityType.CorpUser, EntityType.CorpGroup],
    icon: <User />,
};

const DOMAINS_FILTER: FilterField = {
    field: DOMAINS_FILTER_NAME,
    get displayName() {
        return FIELD_TO_LABEL[DOMAINS_FILTER_NAME];
    },
    type: FieldType.ENTITY,
    entityTypes: [EntityType.Domain],
    icon: <Globe />,
};

const TAGS_FILTER: FilterField = {
    field: TAGS_FILTER_NAME,
    get displayName() {
        return FIELD_TO_LABEL[TAGS_FILTER_NAME];
    },
    type: FieldType.ENTITY,
    entityTypes: [EntityType.Tag],
    icon: <Tag />,
};

const GLOSSARY_TERMS_FILTER: FilterField = {
    field: GLOSSARY_TERMS_FILTER_NAME,
    get displayName() {
        return FIELD_TO_LABEL[GLOSSARY_TERMS_FILTER_NAME];
    },
    type: FieldType.ENTITY,
    entityTypes: [EntityType.GlossaryTerm],
    icon: <BookmarkSimple />,
};

const CONTAINER_FILTER: FilterField = {
    field: CONTAINER_FILTER_NAME,
    get displayName() {
        return FIELD_TO_LABEL[CONTAINER_FILTER_NAME];
    },
    type: FieldType.ENTITY,
    entityTypes: [EntityType.Container],
    icon: <Folder />,
};

export const PARENT_DOCUMENT_FILTER: FilterField = {
    field: PARENT_DOCUMENT_FILTER_NAME,
    get displayName() {
        return FIELD_TO_LABEL[PARENT_DOCUMENT_FILTER_NAME];
    },
    type: FieldType.ENTITY,
    entityTypes: [EntityType.Document],
    icon: <FileText />,
};

const FIELD_PATHS_FILTER: FilterField = {
    field: FIELD_PATHS_FILTER_NAME,
    get displayName() {
        return FIELD_TO_LABEL[FIELD_PATHS_FILTER_NAME];
    },
    type: FieldType.TEXT,
    icon: <Layout />,
};

const FIELD_TAGS_FILTER: FilterField = {
    field: FIELD_TAGS_FILTER_NAME,
    get displayName() {
        return FIELD_TO_LABEL[FIELD_TAGS_FILTER_NAME];
    },
    type: FieldType.ENTITY,
    entityTypes: [EntityType.Tag],
    icon: <Tag />,
};

const FIELD_GLOSSARY_TERMS_FILTER: FilterField = {
    field: FIELD_GLOSSARY_TERMS_FILTER_NAME,
    get displayName() {
        return FIELD_TO_LABEL[FIELD_GLOSSARY_TERMS_FILTER_NAME];
    },
    type: FieldType.ENTITY,
    entityTypes: [EntityType.GlossaryTerm],
    icon: <BookmarkSimple />,
};

const DESCRIPTION_FILTER: FilterField = {
    field: DESCRIPTION_FILTER_NAME,
    get displayName() {
        return FIELD_TO_LABEL[DESCRIPTION_FILTER_NAME];
    },
    type: FieldType.TEXT,
    icon: <FileText />,
};

const FIELD_DESCRIPTIONS_FILTER: FilterField = {
    field: FIELD_DESCRIPTIONS_FILTER_NAME,
    get displayName() {
        return FIELD_TO_LABEL[FIELD_DESCRIPTIONS_FILTER_NAME];
    },
    type: FieldType.TEXT,
    icon: <FileText />,
};

const REMOVED_FILTER: FilterField = {
    field: REMOVED_FILTER_NAME,
    get displayName() {
        return FIELD_TO_LABEL[REMOVED_FILTER_NAME];
    },
    type: FieldType.BOOLEAN,
    icon: <Trash />,
};

const HAS_ACTIVE_INCIDENTS_FILTER: FilterField = {
    field: HAS_ACTIVE_INCIDENTS_FILTER_NAME,
    get displayName() {
        return FIELD_TO_LABEL[HAS_ACTIVE_INCIDENTS_FILTER_NAME];
    },
    type: FieldType.BOOLEAN,
    icon: <Warning />,
};

const ORIGIN_FILTER: FilterField = {
    field: ORIGIN_FILTER_NAME,
    get displayName() {
        return FIELD_TO_LABEL[ORIGIN_FILTER_NAME];
    },
    type: FieldType.ENUM,
    icon: <MapPin />,
};

const DATA_PLATFORM_INSTANCE_FILTER: FilterField = {
    field: DATA_PLATFORM_INSTANCE_FILTER_NAME,
    get displayName() {
        return FIELD_TO_LABEL[DATA_PLATFORM_INSTANCE_FILTER_NAME];
    },
    type: FieldType.ENTITY,
    icon: <Database />,
    entityTypes: [EntityType.DataPlatformInstance],
};

const DATA_PRODUCT_FILTER: FilterField = {
    field: DATA_PRODUCT_FILTER_NAME,
    get displayName() {
        return FIELD_TO_LABEL[DATA_PRODUCT_FILTER_NAME];
    },
    type: FieldType.ENTITY,
    entityTypes: [EntityType.DataProduct],
};

export const STRUCTURED_PROPERTY_FILTER: FilterField = {
    field: STRUCTURED_PROPERTIES_FILTER_NAME,
    get displayName() {
        return FIELD_TO_LABEL[STRUCTURED_PROPERTIES_FILTER_NAME];
    },
    type: FieldType.TEXT,
    icon: <TableIcon />,
};

const HAS_SIBLINGS_FILTER: FilterField = {
    field: HAS_SIBLINGS_FILTER_NAME,
    get displayName() {
        return FIELD_TO_LABEL[HAS_SIBLINGS_FILTER_NAME];
    },
    type: FieldType.BOOLEAN,
    icon: <Wrench />,
};

const DAY_IN_MILLIS = 24 * 60 * 60 * 1000;

export const LAST_MODIFIED_FILTER: FilterField = {
    field: LAST_MODIFIED_FILTER_NAME,
    get displayName() {
        return FIELD_TO_LABEL[LAST_MODIFIED_FILTER_NAME];
    },
    type: FieldType.BUCKETED_TIMESTAMP,
    icon: <Timer size="1em" />,
    useDatePicker: true,
    get options() {
        return [
            {
                label: i18next.t('search:filters.dateRange.last1Day'),
                startOffsetMillis: DAY_IN_MILLIS,
            },
            {
                label: i18next.t('search:filters.dateRange.last3Days'),
                startOffsetMillis: 3 * DAY_IN_MILLIS,
            },
            {
                label: i18next.t('search:filters.dateRange.lastWeek'),
                startOffsetMillis: 7 * DAY_IN_MILLIS,
            },
            {
                label: i18next.t('search:filters.dateRange.lastTwoWeeks'),
                startOffsetMillis: 14 * DAY_IN_MILLIS,
            },
            {
                label: i18next.t('search:filters.dateRange.lastMonth'),
                startOffsetMillis: 31 * DAY_IN_MILLIS,
            },
            {
                label: i18next.t('search:filters.dateRange.last3Months'),
                startOffsetMillis: 92 * DAY_IN_MILLIS,
            },
            {
                label: i18next.t('search:filters.dateRange.last6Months'),
                startOffsetMillis: 184 * DAY_IN_MILLIS,
            },
            {
                label: i18next.t('search:filters.dateRange.lastYear'),
                startOffsetMillis: 365 * DAY_IN_MILLIS,
            },
        ];
    },
};

const BROWSE_FILTER: FilterField = {
    field: BROWSE_PATH_V2_FILTER_NAME,
    get displayName() {
        return FIELD_TO_LABEL[BROWSE_PATH_V2_FILTER_NAME];
    },
    type: FieldType.BROWSE_PATH,
    icon: <Folder />,
};

export const DEFAULT_FILTER_FIELDS: FilterField[] = [
    ENTITY_SUB_TYPE_FILTER,
    PLATFORM_FILTER,
    OWNERS_FILTER,
    DOMAINS_FILTER,
    DATA_PRODUCT_FILTER,
    LAST_MODIFIED_FILTER,
    TAGS_FILTER,
    GLOSSARY_TERMS_FILTER,
    CONTAINER_FILTER,
    PARENT_DOCUMENT_FILTER,
    FIELD_PATHS_FILTER,
    FIELD_TAGS_FILTER,
    FIELD_GLOSSARY_TERMS_FILTER,
    DESCRIPTION_FILTER,
    FIELD_DESCRIPTIONS_FILTER,
    REMOVED_FILTER,
    HAS_ACTIVE_INCIDENTS_FILTER,
    ORIGIN_FILTER,
    DATA_PLATFORM_INSTANCE_FILTER,
    HAS_SIBLINGS_FILTER,
];

export const ALL_FILTER_FIELDS: FilterField[] = [
    ...DEFAULT_FILTER_FIELDS,
    ENTITY_TYPE_FILTER,
    TYPE_NAMES_FILTER,
    BROWSE_FILTER,
];
