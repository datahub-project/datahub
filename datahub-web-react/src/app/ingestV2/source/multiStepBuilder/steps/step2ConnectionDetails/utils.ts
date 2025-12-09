export function encodeSecret(secretName: string) {
    return `\${${secretName}}`;
}

export function decodeSecret(encodedSecret: string) {
    if (!encodedSecret) return encodedSecret;
    if (encodedSecret.startsWith('${') && encodedSecret.endsWith('}')) {
        return encodedSecret.slice(2, encodedSecret.length - 1);
    }
    return encodedSecret;
}
