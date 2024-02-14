import React from 'react';
import {
    CloseCircleOutlined,
    DeleteOutlined,
    FileOutlined,
    FileTextOutlined,
    FolderOutlined,
    LayoutOutlined,
    WarningOutlined,
} from '@ant-design/icons';
import { EntityType } from '../../../../types.generated';
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
    HAS_FAILING_ASSERTIONS_FILTER_NAME,
    ORIGIN_FILTER_NAME,
    OWNERS_FILTER_NAME,
    PLATFORM_FILTER_NAME,
    REMOVED_FILTER_NAME,
    TAGS_FILTER_NAME,
    TYPE_NAMES_FILTER_NAME,
} from '../../utils/constants';
import { FieldType, FilterField } from '../types';

export const ENTITY_SUB_TYPE_FILTER = {
    field: ENTITY_SUB_TYPE_FILTER_NAME,
    displayName: FIELD_TO_LABEL[ENTITY_SUB_TYPE_FILTER_NAME],
    type: FieldType.NESTED_ENTITY_TYPE,
    icon: <FileOutlined />,
};

export const ENTITY_TYPE_FILTER = {
    field: ENTITY_FILTER_NAME,
    displayName: FIELD_TO_LABEL[ENTITY_FILTER_NAME],
    type: FieldType.ENTITY_TYPE,
    icon: <FileOutlined />,
};

export const TYPE_NAMES_FILTER = {
    field: TYPE_NAMES_FILTER_NAME,
    displayName: FIELD_TO_LABEL[TYPE_NAMES_FILTER_NAME],
    type: FieldType.ENUM,
    icon: <FileOutlined />,
};

export const PLATFORM_FILTER = {
    field: PLATFORM_FILTER_NAME,
    displayName: FIELD_TO_LABEL[PLATFORM_FILTER_NAME],
    type: FieldType.ENTITY,
    entityTypes: [EntityType.DataPlatform],
};

export const OWNERS_FILTER = {
    field: OWNERS_FILTER_NAME,
    displayName: FIELD_TO_LABEL[OWNERS_FILTER_NAME],
    type: FieldType.ENTITY,
    entityTypes: [EntityType.CorpUser, EntityType.CorpGroup],
};

export const DOMAINS_FILTER = {
    field: DOMAINS_FILTER_NAME,
    displayName: FIELD_TO_LABEL[DOMAINS_FILTER_NAME],
    type: FieldType.ENTITY,
    entityTypes: [EntityType.Domain],
};

export const TAGS_FILTER = {
    field: TAGS_FILTER_NAME,
    displayName: FIELD_TO_LABEL[TAGS_FILTER_NAME],
    type: FieldType.ENTITY,
    entityTypes: [EntityType.Tag],
};

export const GLOSSARY_TERMS_FILTER = {
    field: GLOSSARY_TERMS_FILTER_NAME,
    displayName: FIELD_TO_LABEL[GLOSSARY_TERMS_FILTER_NAME],
    type: FieldType.ENTITY,
    entityTypes: [EntityType.GlossaryTerm],
};

export const CONTAINER_FILTER = {
    field: CONTAINER_FILTER_NAME,
    displayName: FIELD_TO_LABEL[CONTAINER_FILTER_NAME],
    type: FieldType.ENTITY,
    entityTypes: [EntityType.Container],
};

export const FIELD_PATHS_FILTER = {
    field: FIELD_PATHS_FILTER_NAME,
    displayName: FIELD_TO_LABEL[FIELD_PATHS_FILTER_NAME],
    type: FieldType.TEXT,
    icon: <LayoutOutlined />,
};

export const FIELD_TAGS_FILTER = {
    field: FIELD_TAGS_FILTER_NAME,
    displayName: FIELD_TO_LABEL[FIELD_TAGS_FILTER_NAME],
    type: FieldType.ENTITY,
    entityTypes: [EntityType.Tag],
};

export const FIELD_GLOSSARY_TERMS_FILTER = {
    field: FIELD_GLOSSARY_TERMS_FILTER_NAME,
    displayName: FIELD_TO_LABEL[FIELD_GLOSSARY_TERMS_FILTER_NAME],
    type: FieldType.ENTITY,
    entityTypes: [EntityType.GlossaryTerm],
};

export const DESCRIPTION_FILTER = {
    field: DESCRIPTION_FILTER_NAME,
    displayName: FIELD_TO_LABEL[DESCRIPTION_FILTER_NAME],
    type: FieldType.TEXT,
    icon: <FileTextOutlined />,
};

export const FIELD_DESCRIPTIONS_FILTER = {
    field: FIELD_DESCRIPTIONS_FILTER_NAME,
    displayName: FIELD_TO_LABEL[FIELD_DESCRIPTIONS_FILTER_NAME],
    type: FieldType.TEXT,
    icon: <FileTextOutlined />,
};

export const REMOVED_FILTER = {
    field: REMOVED_FILTER_NAME,
    displayName: FIELD_TO_LABEL[REMOVED_FILTER_NAME],
    type: FieldType.BOOLEAN,
    icon: <DeleteOutlined />,
};

export const HAS_ACTIVE_INCIDENTS_FILTER = {
    field: HAS_ACTIVE_INCIDENTS_FILTER_NAME,
    displayName: FIELD_TO_LABEL[HAS_ACTIVE_INCIDENTS_FILTER_NAME],
    type: FieldType.BOOLEAN,
    icon: <WarningOutlined />,
};

export const HAS_FAILING_ASSERTIONS_FILTER = {
    field: HAS_FAILING_ASSERTIONS_FILTER_NAME,
    displayName: FIELD_TO_LABEL[HAS_FAILING_ASSERTIONS_FILTER_NAME],
    type: FieldType.BOOLEAN,
    icon: <CloseCircleOutlined />,
};

export const ORIGIN_FILTER = {
    field: ORIGIN_FILTER_NAME,
    displayName: FIELD_TO_LABEL[ORIGIN_FILTER_NAME],
    type: FieldType.ENUM,
};

export const DATA_PLATFORM_INSTANCE_FILTER = {
    field: DATA_PLATFORM_INSTANCE_FILTER_NAME,
    displayName: FIELD_TO_LABEL[DATA_PLATFORM_INSTANCE_FILTER_NAME],
    type: FieldType.ENTITY,
    entityTypes: [EntityType.DataPlatformInstance],
};

export const DATA_PRODUCT_FILTER = {
    field: DATA_PRODUCT_FILTER_NAME,
    displayName: FIELD_TO_LABEL[DATA_PRODUCT_FILTER_NAME],
    type: FieldType.ENTITY,
    entityTypes: [EntityType.DataProduct],
};

export const BROWSE_FILTER = {
    field: BROWSE_PATH_V2_FILTER_NAME,
    displayName: FIELD_TO_LABEL[BROWSE_PATH_V2_FILTER_NAME],
    type: FieldType.BROWSE_PATH,
    icon: <FolderOutlined />,
};

export const DEFAULT_FILTER_FIELDS: FilterField[] = [
    ENTITY_SUB_TYPE_FILTER,
    PLATFORM_FILTER,
    OWNERS_FILTER,
    DOMAINS_FILTER,
    DATA_PRODUCT_FILTER,
    TAGS_FILTER,
    GLOSSARY_TERMS_FILTER,
    CONTAINER_FILTER,
    FIELD_PATHS_FILTER,
    FIELD_TAGS_FILTER,
    FIELD_GLOSSARY_TERMS_FILTER,
    DESCRIPTION_FILTER,
    FIELD_DESCRIPTIONS_FILTER,
    REMOVED_FILTER,
    HAS_ACTIVE_INCIDENTS_FILTER,
    HAS_FAILING_ASSERTIONS_FILTER,
    ORIGIN_FILTER,
    DATA_PLATFORM_INSTANCE_FILTER,
];

export const VIEW_BUILDER_FIELDS: FilterField[] = [
    ENTITY_TYPE_FILTER,
    TYPE_NAMES_FILTER,
    PLATFORM_FILTER,
    OWNERS_FILTER,
    DOMAINS_FILTER,
    DATA_PRODUCT_FILTER,
    TAGS_FILTER,
    GLOSSARY_TERMS_FILTER,
    CONTAINER_FILTER,
    FIELD_PATHS_FILTER,
    FIELD_TAGS_FILTER,
    FIELD_GLOSSARY_TERMS_FILTER,
    DESCRIPTION_FILTER,
    FIELD_DESCRIPTIONS_FILTER,
    REMOVED_FILTER,
    HAS_ACTIVE_INCIDENTS_FILTER,
    HAS_FAILING_ASSERTIONS_FILTER,
    ORIGIN_FILTER,
    DATA_PLATFORM_INSTANCE_FILTER,
];

export const ALL_FILTER_FIELDS: FilterField[] = [
    ...DEFAULT_FILTER_FIELDS,
    ENTITY_TYPE_FILTER,
    TYPE_NAMES_FILTER,
    BROWSE_FILTER,
];
