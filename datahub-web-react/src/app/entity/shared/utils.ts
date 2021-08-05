export function urlEncodeUrn(urn: string) {
    return urn && urn.replace(/%/g, '%25').replace(/\//g, '%2F').replace(/\?/g, '%3F').replace(/#/g, '%23');
}

export function notEmpty<TValue>(value: TValue | null | undefined): value is TValue {
    return value !== null && value !== undefined;
}
