import { useMemo } from 'react';
import { EditableSchemaMetadata, Maybe, SchemaField } from '../../../types.generated';
import { getFieldRelevantDescription } from '../../entity/dataset/profile/schema/utils/utils';
import { removeMarkdown } from '../../entity/shared/components/styled/StripMarkdownText';

export default function useFieldDescription(
    field: SchemaField,
    editableSchemaMetadata: Maybe<EditableSchemaMetadata> | undefined,
): string {
    return useMemo(() => {
        const fieldRelevantDescription = getFieldRelevantDescription(field, editableSchemaMetadata);

        return fieldRelevantDescription ? removeMarkdown(fieldRelevantDescription) : 'unknown';
    }, [editableSchemaMetadata, field]);
}
