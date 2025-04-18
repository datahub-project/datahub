import { addInterval } from '../timeUtils';
import { DateInterval } from '../../../../types.generated';

describe('timeUtils', () => {
    describe('addInterval', () => {
        it('add date interval works correctly', () => {
            const input = new Date(1677242304000);
            const afterAdd = addInterval(1, input, DateInterval.Month);
            const expected = new Date(1679661504000);
            expect(afterAdd.getTime()).toEqual(expected.getTime());
        });
    });
});
