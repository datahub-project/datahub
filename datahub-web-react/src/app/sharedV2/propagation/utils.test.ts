import { hasPropagationDetails } from '@app/sharedV2/propagation/utils';

import { StringMapEntry } from '@types';

function attributionWith(sourceDetail: StringMapEntry[]) {
    return {
        attribution: {
            actor: { urn: 'urn:li:corpuser:test', type: undefined } as any,
            time: 0,
            sourceDetail,
        },
    };
}

describe('hasPropagationDetails', () => {
    it('is false when there is no attribution', () => {
        expect(hasPropagationDetails(undefined)).toBe(false);
        expect(hasPropagationDetails({})).toBe(false);
    });

    it('is true for propagated attribution', () => {
        expect(hasPropagationDetails(attributionWith([{ key: 'propagated', value: 'true' }]))).toBe(true);
    });

    // Regression: externally-ingested (e.g. Lake Formation) associations render an "Ingested"
    // block in HoverCardAttributionDetails, so the predicate that gates mounting it must agree.
    it('is true for externally-ingested attribution', () => {
        expect(hasPropagationDetails(attributionWith([{ key: 'external', value: 'true' }]))).toBe(true);
    });

    it('is false for a plain (non-propagated, non-external) attribution', () => {
        expect(hasPropagationDetails(attributionWith([{ key: 'origin', value: 'some-source' }]))).toBe(false);
    });

    it('is true when the propagation context marks it propagated', () => {
        expect(hasPropagationDetails({ context: JSON.stringify({ propagated: true }) })).toBe(true);
    });
});
