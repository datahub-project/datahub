import { print } from 'graphql';
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
        expect(parseGql).not.toHaveBeenCalled();
        expect(doc.definitions).toHaveLength(1);
        expect(parseGql).toHaveBeenCalledTimes(1);
        expect(doc.loc?.source.body).toContain('LazyGqlProbe');
        expect(parseGql).toHaveBeenCalledTimes(1);
    });

    it('interpolates lazy fragment documents the way codegen emits them', async () => {
        const { gql } = await import('@graphql/lazyGql');
        const fragment = gql`
            fragment LazyGqlFragment on Query {
                __typename
            }
        `;
        const query = gql`
            query LazyGqlWithFragment {
                ...LazyGqlFragment
            }
            ${fragment}
        `;

        expect(query.definitions.map((definition) => definition.kind)).toEqual([
            'OperationDefinition',
            'FragmentDefinition',
        ]);
    });

    it('behaves like a plain DocumentNode when frozen, cloned or serialized', async () => {
        const { gql } = await import('@graphql/lazyGql');
        const doc = gql`
            query LazyGqlPlain {
                __typename
            }
        `;
        const expected = print(doc);

        expect(print(structuredClone(doc))).toBe(expected);
        expect(print({ ...doc })).toBe(expected);
        expect(JSON.parse(JSON.stringify(doc)).definitions).toHaveLength(1);
        expect(print(Object.freeze(doc))).toBe(expected);
    });
});
