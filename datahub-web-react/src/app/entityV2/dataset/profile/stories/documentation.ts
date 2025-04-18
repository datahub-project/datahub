import { EntityType } from '../../../../../types.generated';

export const sampleDocs = [
    {
        url: 'https://www.google.com',
        description: 'This doc spans the internet web',
        author: { urn: 'urn:li:corpuser:1', username: '1', type: EntityType.CorpUser },
        created: {
            time: 0,
            actor: 'urn:li:corpuser:1',
        },
    },
];
