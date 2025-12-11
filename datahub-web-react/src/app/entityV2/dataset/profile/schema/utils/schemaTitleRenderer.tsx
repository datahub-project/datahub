/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';

import { InteriorTitleContent } from '@app/entityV2/dataset/profile/schema/components/InteriorTitleContent';
import { ExtendedSchemaFields } from '@app/entityV2/dataset/profile/schema/utils/types';

import { SchemaMetadata } from '@types';

export default function useSchemaTitleRenderer(
    parentUrn: string,
    schemaMetadata: SchemaMetadata | undefined | null,
    filterText: string,
    isCompact?: boolean,
) {
    return (fieldPath: string, record: ExtendedSchemaFields): JSX.Element => {
        return (
            <InteriorTitleContent
                parentUrn={parentUrn}
                isCompact={isCompact}
                filterText={filterText}
                fieldPath={fieldPath}
                record={record}
                schemaMetadata={schemaMetadata}
            />
        );
    };
}
