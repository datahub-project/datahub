import { Document } from '@types';

// Shared by DocumentEntity and DocumentExternalProfile. The profile is loaded through import() from
// the entity, so importing the entity class back from the profile would close a chunk cycle.
export function getDocumentOverrideProperties(data: Document) {
    const externalUrl = data.info?.source?.externalUrl;
    return {
        name: data.info?.title,
        externalUrl,
        lastIngested: data.lastIngested ?? undefined,
        properties: {
            name: data.info?.title,
            externalUrl,
        },
    };
}
