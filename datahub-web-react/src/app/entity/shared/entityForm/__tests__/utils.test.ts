import { FormView } from '@app/entity/shared/entityForm/EntityFormContext';
import { generateFormCompletionFilter } from '@app/entity/shared/entityForm/utils';

describe('form utils tests', () => {
    it('should create the correct form completion filter for bulk verify view', () => {
        const formCompletionFilter = generateFormCompletionFilter(FormView.BULK_VERIFY, true);
        expect(formCompletionFilter).toMatchObject({ isFormComplete: true, isFormVerified: false });
    });

    it('should create the correct form completion filter for verification forms', () => {
        const formCompletionFilter = generateFormCompletionFilter(FormView.BY_QUESTION, true);
        expect(formCompletionFilter).toMatchObject({ isFormVerified: false });
    });

    it('should create the correct form completion filter for completion forms', () => {
        const formCompletionFilter = generateFormCompletionFilter(FormView.BY_QUESTION, false);
        expect(formCompletionFilter).toMatchObject({ isFormComplete: false });
    });
});
