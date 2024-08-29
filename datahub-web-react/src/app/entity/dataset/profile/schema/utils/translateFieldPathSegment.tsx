import { ARRAY_TOKEN, UNION_TOKEN } from './constants';

export default function translateFieldPathSegment(fieldPathSegment, i, fieldPathParts) {
    // for each segment, convert its fieldPath representation into a human readable version
    // We leave the annotations present and strip them out in a second pass
    const previousSegment = fieldPathParts[i - 1];

    // we need to look back to see how many arrays there were previously to display the array indexing notation after the field
    let previousArrayCount = 0;
    for (let j = i - 1; j >= 0; j--) {
        if (fieldPathParts[j] === ARRAY_TOKEN) {
            previousArrayCount++;
        }
        if (fieldPathParts[j].indexOf('[') === -1) {
            break;
        }
    }

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
        const typeName = fieldPathSegment.replace(/\[type=/g, '').replace(/\]/g, '');
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

    // arrays are represented as [type=array]array_field_name
    // we convert into array_field_name[]
    if (previousArrayCount > 0) {
        return `${fieldPathSegment}${'[]'.repeat(previousArrayCount)}.`;
    }

    return `${fieldPathSegment}.`;
}
