export function encodeSecret(secretName: string) {
    return `\${${secretName}}`;
}
