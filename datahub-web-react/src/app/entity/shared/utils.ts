export function urlEncodeUrn(urn: string) {
    return urn?.replaceAll('/', '%2F').replaceAll('?', '%3F').replaceAll('%', '%25').replaceAll('#', '%23');
}
