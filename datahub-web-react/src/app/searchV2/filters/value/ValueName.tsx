import moment from 'moment';
import React from 'react';
import { Typography } from 'antd';
import { FilterField, FilterValue, FieldType } from '../types';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { getEntityTypeFilterValueDisplayName } from './utils';
import { UNIT_SEPARATOR } from '../../utils/constants';
import { getV1FieldPathFromSchemaFieldUrn } from '../../../lineageV2/lineageUtils';
import { getStructuredPropFilterDisplayName } from '../utils';

function getTextFieldName(field: FilterField, value: FilterValue) {
    let textFieldName = value.displayName || value.value;
    if (textFieldName.startsWith('urn:li:schemaField:')) {
        textFieldName = getV1FieldPathFromSchemaFieldUrn(textFieldName);
    }
    return getStructuredPropFilterDisplayName(field.field, value.value, field.entity) || textFieldName;
}

interface Props {
    field: FilterField;
    value: FilterValue;
}

export default function ValueName({ field, value }: Props) {
    const entityRegistry = useEntityRegistry();

    switch (field.type) {
        case FieldType.TEXT:
        case FieldType.BOOLEAN:
        case FieldType.ENUM:
            return <>{getTextFieldName(field, value)}</>;
        case FieldType.ENTITY:
            return (
                <>{(value.entity && entityRegistry.getDisplayName(value.entity?.type, value.entity)) || undefined}</>
            );
        case FieldType.ENTITY_TYPE:
        case FieldType.NESTED_ENTITY_TYPE:
            return <> {getEntityTypeFilterValueDisplayName(value.value, entityRegistry)}</>;
        case FieldType.BROWSE_PATH: {
            // TODO: Break this into a separate component.
            const pathParts = value.value.split(UNIT_SEPARATOR).filter((part) => part);
            return (
                <>
                    {pathParts.map((part, index) => (
                        <>
                            {part}
                            {(index < pathParts.length - 1 && (
                                <Typography.Text type="secondary"> / </Typography.Text>
                            )) ||
                                undefined}
                        </>
                    ))}
                </>
            );
        }
        case FieldType.BUCKETED_TIMESTAMP:
            // Note: Currently unused, as SelectedFilter.tsx renders DatePicker instead
            return <>{moment(value.value).format('YYYY-MM-DD')}</>;
        default:
            console.error(`Unknown field type: ${field}`);
            return <>n/a</>;
    }
}
