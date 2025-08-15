import React from 'react';

import StructuredPropertyValue from '@app/entity/shared/tabs/Properties/StructuredPropertyValue';
import { PropertyRow } from '@app/entity/shared/tabs/Properties/types';

import { StdDataType } from '@types';

interface Props {
    propertyRow: PropertyRow;
    filterText?: string;
}

export default function ValuesColumn({ propertyRow, filterText }: Props) {
    const { values } = propertyRow;
    const isRichText = propertyRow.dataType?.info?.type === StdDataType.RichText;

    return (
        <>
            {values ? (
                values.map((v) => <StructuredPropertyValue value={v} isRichText={isRichText} filterText={filterText} />)
            ) : (
                <span />
            )}
        </>
    );
}
