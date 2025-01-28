import { FIELD_TO_LABEL, ORDERED_FIELDS } from '../utils/constants';

describe('constants', () => {
    it('ensure that all ordered fields have a corresponding label', () => {
        expect(ORDERED_FIELDS.filter((field) => !Object.keys(FIELD_TO_LABEL).includes(field)).length).toEqual(0);
    });
});
