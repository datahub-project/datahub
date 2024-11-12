import StructuredPropertyValue from '@src/app/entityV2/shared/tabs/Properties/StructuredPropertyValue';
import { mapStructuredPropertyToPropertyRow } from '@src/app/entityV2/shared/tabs/Properties/useStructuredProperties';
import { SchemaFieldEntity, SearchResult, StdDataType } from '@src/types.generated';
import React from 'react';

interface Props {
    record: SchemaFieldEntity;
    propColumn: SearchResult | undefined;
}

const StructuredPropValues = ({ record, propColumn }: Props) => {
    const property = record.structuredProperties?.properties?.find(
        (prop) => prop.structuredProperty.urn === propColumn?.entity.urn,
    );
    const propRow = property ? mapStructuredPropertyToPropertyRow(property) : undefined;
    const values = propRow?.values;
    const isRichText = propRow?.dataType?.info.type === StdDataType.RichText;

    return (
        <>
            {values && (
                <>
                    {values.map((val) => (
                        <StructuredPropertyValue value={val} isRichText={isRichText} />
                    ))}
                </>
            )}
        </>
    );
};

export default StructuredPropValues;
