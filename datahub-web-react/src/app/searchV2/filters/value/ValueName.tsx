import React from 'react';
import { Typography } from 'antd';
import { FilterField, FilterValue, FieldType } from '../types';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { getEntityTypeFilterValueDisplayName } from './utils';
import { UNIT_SEPARATOR } from '../../utils/constants';

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
            return <>{value.value}</>;
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
        default:
            console.error(`Unknown field type: ${field.type}`);
            return <>n/a</>;
    }
}
