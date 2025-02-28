import React from 'react';
import { SchemaMetadata } from '../../../../../../types.generated';
import { InteriorTitleContent } from '../components/InteriorTitleContent';
import { ExtendedSchemaFields } from './types';

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
