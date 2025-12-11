/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
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
