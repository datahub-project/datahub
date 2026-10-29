import type { DocumentNode } from 'graphql';
import parseGql from 'graphql-tag';

/**
 * Drop-in `gql` that defers graphql-tag parsing until the DocumentNode is first
 * read. Codegen emits hundreds of documents into the signed-in shell; parsing
 * them during module evaluation dominates the pre-paint long task (CAT-3693).
 * Apollo only needs the AST when a hook runs, which is after first paint for
 * almost all of those documents.
 *
 * Returns a plain object with lazy getters rather than a Proxy, so freezing,
 * structured cloning, spreading and JSON serialization behave as they do for
 * a normal DocumentNode. `kind` is constant, so graphql-tag's fragment
 * interpolation check (`arg.kind === 'Document'`) does not force a parse.
 */
export function gql(literals: ReadonlyArray<string> | Readonly<string>, ...placeholders: unknown[]): DocumentNode {
    let parsed: DocumentNode | undefined;
    const parse = (): DocumentNode => {
        if (!parsed) {
            parsed = parseGql(literals, ...placeholders);
        }
        return parsed;
    };

    return Object.defineProperties(
        {},
        {
            kind: { value: 'Document', enumerable: true },
            definitions: { get: () => parse().definitions, enumerable: true },
            loc: { get: () => parse().loc, enumerable: true },
        },
    ) as DocumentNode;
}

export default gql;
