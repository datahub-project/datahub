import React from 'react';
import { ExtendedSchemaFields } from './types';
import { SchemaMetadata } from '../../../../../../types.generated';
import { InteriorTitleContent } from '../components/InteriorTitleContent';

export default function useSchemaTitleRenderer(
    schemaMetadata: SchemaMetadata | undefined | null,
    filterText: string,
    isCompact?: boolean,
) {
    return (fieldPath: string, record: ExtendedSchemaFields): JSX.Element => {
        return (
            <InteriorTitleContent
                isCompact={isCompact}
                filterText={filterText}
                fieldPath={fieldPath}
                record={record}
                schemaMetadata={schemaMetadata}
            />
        );
    };
}
