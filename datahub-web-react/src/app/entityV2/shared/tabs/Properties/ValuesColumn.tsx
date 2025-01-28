import React from 'react';
import { StdDataType } from '../../../../../types.generated';
import StructuredPropertyValue from './StructuredPropertyValue';
import { PropertyRow } from './types';

interface Props {
    propertyRow: PropertyRow;
    filterText?: string;
}

export default function ValuesColumn({ propertyRow, filterText }: Props) {
    const { values } = propertyRow;
    const isRichText = propertyRow.dataType?.info.type === StdDataType.RichText;

    return (
        <>
            {values ? (
                values.map((v) => (
                    <StructuredPropertyValue value={v} isRichText={isRichText} filterText={filterText} truncateText />
                ))
            ) : (
                <span />
            )}
        </>
    );
}
