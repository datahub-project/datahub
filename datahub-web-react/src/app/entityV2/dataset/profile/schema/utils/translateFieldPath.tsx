import { KEY_SCHEMA_PREFIX, VERSION_PREFIX } from './constants';
import translateFieldPathSegment from './translateFieldPathSegment';

export default function translateFieldPath(fieldPath: string) {
    // fields that are part of a key schema are prefixed with [key=true]
    // we don't want to display this
    const cleanedFieldPath = fieldPath.replace(KEY_SCHEMA_PREFIX, '').replace(VERSION_PREFIX, '');
    const fieldPathParts = cleanedFieldPath.split('.');

    // convert each fieldPathSegment into a human readable format
    const adjustedFieldPathParts = fieldPathParts.map(translateFieldPathSegment);

    let fieldPathWithoutAnnotations = adjustedFieldPathParts.join('');

    // clean up artifacts from unions and arrays nested within one another
    fieldPathWithoutAnnotations = fieldPathWithoutAnnotations.replace(/\.\./g, '.').replace(/\. /g, ' ');

    // removing a hanging dot if present
    if (fieldPathWithoutAnnotations.endsWith('.')) {
        fieldPathWithoutAnnotations = fieldPathWithoutAnnotations.slice(0, -1);
    }

    return fieldPathWithoutAnnotations;
}
