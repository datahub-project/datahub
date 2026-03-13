// Using 'any' for GraphQL generated types compatibility
// eslint-disable-next-line @typescript-eslint/no-explicit-any
type StructuredProp = any;
// eslint-disable-next-line @typescript-eslint/no-explicit-any
type StructuredPropertyData = any;
// eslint-disable-next-line @typescript-eslint/no-explicit-any
type MutationFn = any;

export function getStructuredValue(
    structuredProps: StructuredProp[],
    qualifiedName: string,
): string | number | undefined {
    const prop = structuredProps.find(
        (p) =>
            p.structuredProperty?.definition?.qualifiedName === qualifiedName ||
            p.structuredProperty?.urn === `urn:li:structuredProperty:${qualifiedName}`,
    );

    if (!prop) return undefined;

    const firstValue = prop.values?.[0];
    if (!firstValue) return undefined;

    if (firstValue.stringValue !== undefined) return firstValue.stringValue;
    if (firstValue.numberValue !== undefined) return firstValue.numberValue;
    if (firstValue.booleanValue !== undefined) return firstValue.booleanValue ? 'true' : 'false';

    return firstValue.value;
}

export function getStructuredList(structuredProps: StructuredProp[], qualifiedName: string): string[] {
    const prop = structuredProps.find(
        (p) =>
            p.structuredProperty?.definition?.qualifiedName === qualifiedName ||
            p.structuredProperty?.urn === `urn:li:structuredProperty:${qualifiedName}`,
    );

    if (!prop || !prop.values) return [];

    return prop.values.map((v) => v.stringValue || v.value || '').filter(Boolean);
}

export function getStructuredPropAllowedValues(props: StructuredPropertyData): string[] {
    return (
        props?.entity?.definition?.allowedValues?.map((item) => item?.value?.stringValue).filter(Boolean) ?? []
    ) as string[];
}

export function buildSchemaFieldUrn(urn: string, fieldPath: string): string {
    return `urn:li:schemaField:(${urn},${fieldPath})`;
}

export function buildStructuredPropertyUrn(qualifiedName: string): string {
    return `urn:li:structuredProperty:${qualifiedName}`;
}

export function createRemoveMutation(
    assetUrn: string,
    targetPropUrn: string,
    mutationFn: MutationFn,
): Promise<unknown> {
    return mutationFn({
        variables: {
            input: {
                assetUrn,
                structuredPropertyUrns: [targetPropUrn],
            },
        },
    });
}

export function createUpsertMutation(
    assetUrn: string,
    properties: Array<{ urn: string; values: string[] }>,
    mutationFn: MutationFn,
): Promise<unknown> {
    return mutationFn({
        variables: {
            input: {
                assetUrn,
                structuredPropertyInputParams: properties.map(({ urn: propUrn, values }) => ({
                    structuredPropertyUrn: propUrn,
                    values: values.map((v) => ({ stringValue: v })),
                })),
            },
        },
    });
}
