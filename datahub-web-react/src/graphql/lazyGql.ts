import type { DocumentNode } from 'graphql';
import parseGql from 'graphql-tag';

/**
 * Drop-in `gql` that defers graphql-tag parsing until the DocumentNode is first
 * read. Codegen emits hundreds of documents into the signed-in shell; parsing
 * them during module evaluation dominates the pre-paint long task (CAT-3693).
 * Apollo only needs the AST when a hook runs, which is after first paint for
 * almost all of those documents.
 */
export function gql(literals: any, ...args: any[]): DocumentNode {
    let doc: DocumentNode | undefined;
    const get = (): DocumentNode => {
        if (!doc) {
            doc = args.length > 0 ? parseGql(literals, ...args) : parseGql(literals);
        }
        return doc;
    };

    return new Proxy({} as DocumentNode, {
        get(_target, prop, receiver) {
            if (prop === Symbol.toStringTag) {
                return 'Object';
            }
            return Reflect.get(get() as object, prop, receiver);
        },
        has(_target, prop) {
            return Reflect.has(get() as object, prop);
        },
        ownKeys() {
            return Reflect.ownKeys(get() as object);
        },
        getOwnPropertyDescriptor(_target, prop) {
            const descriptor = Reflect.getOwnPropertyDescriptor(get() as object, prop);
            // Proxies must report configurable:true for claimed own keys.
            if (descriptor) {
                return { ...descriptor, configurable: true };
            }
            return descriptor;
        },
    });
}

export default gql;

export const { resetCaches } = parseGql;
export const { disableFragmentWarnings } = parseGql;
export const { enableExperimentalFragmentVariables } = parseGql;
export const { disableExperimentalFragmentVariables } = parseGql;
