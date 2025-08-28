import { ARRAY_TOKEN, UNION_TOKEN } from '@app/entityV2/dataset/profile/schema/utils/constants';

export default function translateFieldPathSegment(
    fieldPathSegment: string,
    i: number,
    fieldPathParts: string[],
): string {
    // for each segment, convert its fieldPath representation into a human readable version
    // We leave the annotations present and strip them out in a second pass
    const previousSegment = fieldPathParts[i - 1];

    // strip out the version prefix
    if (
        fieldPathSegment.startsWith('[version=') ||
        fieldPathSegment === ARRAY_TOKEN ||
        fieldPathSegment === UNION_TOKEN
    ) {
        return '';
    }

    // structs that qualify a union are represented as [union]union_field.[type=QualifiedStruct].qualified_struct_field
    // we convert into union_field. (QualifiedStruct) qualified_struct_field
    if (fieldPathSegment.startsWith('[type=') && fieldPathSegment.endsWith(']')) {
        const typeName = fieldPathSegment.replace('[type=', '').replace(']', '');
        // if the qualified struct is the last element, just show the qualified struct
        if (i === fieldPathParts.length - 1) {
            return ` ${typeName}`;
        }

        // if the qualified struct is not the last element, surround with parens
        if (previousSegment === UNION_TOKEN) {
            return `(${typeName}) `;
        }

        // if the struct is not qualifying, ignore
        return '';
    }

    return `${fieldPathSegment}.`;
}
