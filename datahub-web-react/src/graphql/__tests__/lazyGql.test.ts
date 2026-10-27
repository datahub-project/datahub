import { describe, expect, it, vi } from 'vitest';

vi.mock('graphql-tag', async () => {
    const actual = await vi.importActual<typeof import('graphql-tag')>('graphql-tag');
    return {
        ...actual,
        default: vi.fn(actual.default),
    };
});

describe('lazyGql', () => {
    it('does not parse until the DocumentNode is read', async () => {
        const parseGql = (await import('graphql-tag')).default as unknown as ReturnType<typeof vi.fn>;
        const { gql } = await import('@graphql/lazyGql');
        parseGql.mockClear();

        const doc = gql`
            query LazyGqlProbe {
                __typename
            }
        `;

        expect(parseGql).not.toHaveBeenCalled();
        expect(doc.kind).toBe('Document');
        expect(parseGql).toHaveBeenCalledTimes(1);
        expect(doc.definitions).toHaveLength(1);
        expect(parseGql).toHaveBeenCalledTimes(1);
    });
});
