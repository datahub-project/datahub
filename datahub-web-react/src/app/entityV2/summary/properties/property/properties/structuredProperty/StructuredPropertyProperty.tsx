import React, { useMemo } from 'react';

import { useEntityContext } from '@app/entity/shared/EntityContext';
import DateStructuredProperty from '@app/entityV2/summary/properties/property/properties/structuredProperty/components/DateStructuredProperty';
import EntityStructuredProperty from '@app/entityV2/summary/properties/property/properties/structuredProperty/components/EntityStructuredProperty';
import NumberStructuredProperty from '@app/entityV2/summary/properties/property/properties/structuredProperty/components/NumberStructuredProperty';
import StringStructuredProperty from '@app/entityV2/summary/properties/property/properties/structuredProperty/components/StringStructuredProperty';
import UnknownStructuredProperty from '@app/entityV2/summary/properties/property/properties/structuredProperty/components/UnknownStructuredProperty';
import { PropertyComponentProps } from '@app/entityV2/summary/properties/types';
import { DATE_TYPE_URN, NUMBER_TYPE_URN, STRING_TYPE_URN, URN_TYPE_URN } from '@app/shared/constants';

import { StructuredPropertiesEntry } from '@types';

export default function StructuredPropertyProperty(props: PropertyComponentProps) {
    const { property } = props;
    const { entityData } = useEntityContext();

    const structuredPropertyEntry: StructuredPropertiesEntry | undefined = useMemo(() => {
        if (!property.structuredProperty?.urn) return undefined;
        return entityData?.structuredProperties?.properties?.find(
            (entry) => entry.structuredProperty.urn === property.structuredProperty?.urn,
        );
    }, [property.structuredProperty?.urn, entityData]);

    const valueType = structuredPropertyEntry?.structuredProperty?.definition?.valueType?.urn;

    const StructuredPropertyComponent = useMemo(() => {
        switch (valueType) {
            case STRING_TYPE_URN:
                return StringStructuredProperty;
            case NUMBER_TYPE_URN:
                return NumberStructuredProperty;
            case URN_TYPE_URN:
                return EntityStructuredProperty;
            case DATE_TYPE_URN:
                return DateStructuredProperty;
            default:
                // Empty structured property or unsupported type of structured property
                return UnknownStructuredProperty;
        }
    }, [valueType]);

    return <StructuredPropertyComponent {...props} structuredPropertyEntry={structuredPropertyEntry} />;
}
