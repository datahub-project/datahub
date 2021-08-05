export default function translateFieldPathSegment(fieldPathSegment, i, fieldPathParts) {
    // for each segment, convert its fieldPath representation into a human readable version
    // We leave the annotations present and strip them out in a second pass
    const previousSegment = fieldPathParts[i - 1];
    // arrays are represented as [type=array]array_field_name
    // we convert into array_field_name[]
    if (fieldPathSegment.indexOf('[type=array]') >= 0) {
        fieldPathSegment.replace('[type=array]', '');
        return `${fieldPathSegment.replace('[type=array]', '')}[].`;
    }
    // unions are represented as [type=union]union_field_name
    // we convert into union_field_name.
    if (fieldPathSegment.indexOf('[type=union]') >= 0) {
        fieldPathSegment.replace('[type=union]', '');
        return `${fieldPathSegment.replace('[type=union]', '')}.`;
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
        if (previousSegment?.startsWith('[type=union]')) {
            return `(${typeName}) `;
        }

        // if the struct is not qualifying, ignore
        return '';
    }

    // replace any remaining annotations
    // eslint-disable-next-line no-useless-escape
    return `${fieldPathSegment}.`.replace(/([\[]).+?([\]])/g, '');
}
