/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';

import {
    SectionHeader,
    StyledDivider,
} from '@app/entity/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/components';
import useBusinessAttributeRenderer from '@app/entity/shared/tabs/Dataset/Schema/utils/useBusinessAttributeRenderer';
import SchemaEditableContext from '@app/shared/SchemaEditableContext';
import { useBusinessAttributesFlag } from '@app/useAppConfig';

import { SchemaField } from '@types';

interface Props {
    expandedField: SchemaField;
}

export default function FieldAttribute({ expandedField }: Props) {
    const isSchemaEditable = React.useContext(SchemaEditableContext);
    const attributeRenderer = useBusinessAttributeRenderer('', isSchemaEditable);

    const businessAttributesFlag = useBusinessAttributesFlag();

    return businessAttributesFlag ? (
        <>
            <SectionHeader>Business Attribute</SectionHeader>
            {/* pass in globalTags since this is a shared component, tags will not be shown or used */}
            <div data-testid={`schema-field-${expandedField.fieldPath}-businessAttribute`}>
                {attributeRenderer('', expandedField)}
            </div>
            <StyledDivider />
        </>
    ) : null;
}
