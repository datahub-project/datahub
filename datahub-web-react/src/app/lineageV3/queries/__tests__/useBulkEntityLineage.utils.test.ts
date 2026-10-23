import { LINEAGE_FILTER_TYPE, LineageFilter } from '@app/lineageV3/common';
import {
    getNodeForBulkResult,
    getUnfetchedTransformationalLeaves,
} from '@app/lineageV3/queries/useBulkEntityLineage.utils';

import { EntityType, LineageDirection } from '@types';

const REAL_URN = 'urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.dim_b,PROD)';
const ENCRYPTED_URN = 'urn:li:restricted:v2:abc123';

// The helper only reads the node back out, so a minimal stub is enough.
const makeNodes = (...urns: string[]) => new Map(urns.map((urn) => [urn, { urn } as any]));

describe('getNodeForBulkResult', () => {
    it('matches a Restricted result (re-encrypted urn) back to the requested node by position', () => {
        // Node was requested/keyed by its real urn; the result comes back Restricted with a
        // different (encrypted) urn. Without the positional fallback this node stays a skeleton.
        const nodes = makeNodes(REAL_URN);
        const node = getNodeForBulkResult(nodes, ENCRYPTED_URN, EntityType.Restricted, [REAL_URN], 0);
        expect(node?.urn).toBe(REAL_URN);
    });

    it('matches a normal result directly by its urn', () => {
        const nodes = makeNodes(REAL_URN);
        const node = getNodeForBulkResult(nodes, REAL_URN, EntityType.Dataset, [REAL_URN], 0);
        expect(node?.urn).toBe(REAL_URN);
    });

    it('does not fall back positionally for non-Restricted results', () => {
        const nodes = makeNodes(REAL_URN);
        const node = getNodeForBulkResult(nodes, ENCRYPTED_URN, EntityType.Dataset, [REAL_URN], 0);
        expect(node).toBeUndefined();
    });

    it('does not attach when the requested-urn list is unavailable (length-mismatch guard)', () => {
        const nodes = makeNodes(REAL_URN);
        const node = getNodeForBulkResult(nodes, ENCRYPTED_URN, EntityType.Restricted, [], 0);
        expect(node).toBeUndefined();
    });
});

describe('getUnfetchedTransformationalLeaves', () => {
    const dbt = (name: string) => `urn:li:dataset:(urn:li:dataPlatform:dbt,${name},PROD)`;
    const snowflake = (name: string) => `urn:li:dataset:(urn:li:dataPlatform:snowflake,${name},PROD)`;
    const node = (urn: string, fetched = false) => ({
        urn,
        type: EntityType.Dataset,
        entity: fetched ? {} : undefined,
    });
    const filter = (contents: string[]) =>
        new Map([
            ['f', { type: LINEAGE_FILTER_TYPE, direction: LineageDirection.Upstream, contents } as LineageFilter],
        ]);

    it('returns unfetched, hidden transformational leaves', () => {
        const contents = [snowflake('t0'), dbt('shown'), dbt('fetched'), dbt('hidden')];
        const nodes = new Map(
            [node(contents[0]), node(contents[1]), node(contents[2], true), node(contents[3])].map((n) => [
                n.urn,
                n as any,
            ]),
        );

        expect(getUnfetchedTransformationalLeaves(filter(contents), nodes, EntityType.Dataset, [dbt('shown')])).toEqual(
            [dbt('hidden')],
        );
    });
});
