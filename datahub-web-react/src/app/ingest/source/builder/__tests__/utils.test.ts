import { getSchemaValidator } from '../utils';
import {
    validRecipe,
    validRecipeNoSink,
    invalidTypeRecipe,
    missingSourceRecipe,
    invalidNestedFieldRecipe,
} from '../ingestionMocks';

describe('ingestion builder utils', () => {
    it('should return true for a valid recipe', () => {
        const validate = getSchemaValidator();
        const isValid = validate(validRecipe);

        expect(isValid).toBe(true);
    });

    it('should return true for a valid recipe with no sink', () => {
        const validate = getSchemaValidator();
        const isValid = validate(validRecipeNoSink);

        expect(isValid).toBe(true);
    });

    it('should return false for a recipe with an invalid type', () => {
        const validate = getSchemaValidator();
        const isValid = validate(invalidTypeRecipe);

        expect(isValid).toBe(false);
    });

    it('should return false for a recipe missing source', () => {
        const validate = getSchemaValidator();
        const isValid = validate(missingSourceRecipe);

        expect(isValid).toBe(false);
    });

    it('should return false for an invalid recipe due to a nested field', () => {
        const validate = getSchemaValidator();
        const isValid = validate(invalidNestedFieldRecipe);

        expect(isValid).toBe(false);
    });
});
