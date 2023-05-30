import { getMatchPrioritizingPrimary } from '../utils';

const MOCK_MATCHED_FIELDS = [
    {
        name: 'fieldPaths',
        value: 'rain',
    },
    {
        name: 'description',
        value: 'rainbow',
    },
    {
        name: 'fieldPaths',
        value: 'rainbow',
    },
    {
        name: 'fieldPaths',
        value: 'rainbows',
    },
];

describe('utils', () => {
    describe('getMatchPrioritizingPrimary', () => {
        it('prioritizes exact match', () => {
            global.window.location.search = 'query=rainbow';
            const match = getMatchPrioritizingPrimary(MOCK_MATCHED_FIELDS, 'fieldPaths');
            expect(match?.value).toEqual('rainbow');
            expect(match?.name).toEqual('fieldPaths');
        });
        it('will accept first contains match', () => {
            global.window.location.search = 'query=bow';
            const match = getMatchPrioritizingPrimary(MOCK_MATCHED_FIELDS, 'fieldPaths');
            expect(match?.value).toEqual('rainbow');
            expect(match?.name).toEqual('fieldPaths');
        });
    });
});
