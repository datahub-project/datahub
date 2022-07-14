import { setFieldValueOnRecipe, setListValuesOnRecipe } from '../utils';

describe('setFieldValueOnRecipe', () => {
    const accountIdFieldPath = 'source.config.account_id';
    const profilingEnabledFieldPath = 'source.config.profiling.enabled';

    it('should set the field value on a recipe object when it was not defined', () => {
        const recipe = { source: { config: {} } };
        const updatedRecipe = setFieldValueOnRecipe(recipe, 'test', accountIdFieldPath);
        expect(updatedRecipe).toMatchObject({ source: { config: { account_id: 'test' } } });
    });

    it('should update the field value on a recipe object when it was defined', () => {
        const recipe = { source: { config: { account_id: 'test' } } };
        const updatedRecipe = setFieldValueOnRecipe(recipe, 'edited!', accountIdFieldPath);
        expect(updatedRecipe).toMatchObject({ source: { config: { account_id: 'edited!' } } });
    });

    it('should update the field value on a recipe without changing any other fields', () => {
        const recipe = { source: { config: { test: 'test' } } };
        const updatedRecipe = setFieldValueOnRecipe(recipe, 'edited!', accountIdFieldPath);
        expect(updatedRecipe).toMatchObject({ source: { config: { test: 'test', account_id: 'edited!' } } });
    });

    it('should clear the key: value pair when passing in null', () => {
        const recipe = { source: { config: { existingField: true, account_id: 'test' } } };
        const updatedRecipe = setFieldValueOnRecipe(recipe, null, accountIdFieldPath);
        expect(updatedRecipe).toMatchObject({ source: { config: { existingField: true } } });
    });

    it('should return the original recipe when passing in undefined', () => {
        const recipe = { source: { config: { test: 'test' } } };
        const updatedRecipe = setFieldValueOnRecipe(recipe, undefined, accountIdFieldPath);
        expect(updatedRecipe).toMatchObject(recipe);
    });

    it('should set the field value on a recipe object when it was not defined and has a parent', () => {
        const recipe = { source: { config: {} } };
        const updatedRecipe = setFieldValueOnRecipe(recipe, true, profilingEnabledFieldPath);
        expect(updatedRecipe).toMatchObject({ source: { config: { profiling: { enabled: true } } } });
    });

    it('should update the field value on a recipe object when it was defined and has a parent', () => {
        const recipe = { source: { config: { profiling: { enabled: true } } } };
        const updatedRecipe = setFieldValueOnRecipe(recipe, false, profilingEnabledFieldPath);
        expect(updatedRecipe).toMatchObject({ source: { config: { profiling: { enabled: false } } } });
    });

    it('should update the field value with a parent on a recipe without changing any other fields', () => {
        const recipe = { source: { config: { test: 'test' } } };
        const updatedRecipe = setFieldValueOnRecipe(recipe, false, profilingEnabledFieldPath);
        expect(updatedRecipe).toMatchObject({ source: { config: { test: 'test', profiling: { enabled: false } } } });
    });

    it('should clear the field and its parent when passing in null and field is only child of parent', () => {
        const recipe = { source: { config: { test: 'test', profiling: { enabled: true } } } };
        const updatedRecipe = setFieldValueOnRecipe(recipe, null, profilingEnabledFieldPath);
        expect(updatedRecipe).toMatchObject({ source: { config: { test: 'test' } } });
    });

    it('should clear the field but not its parent when passing in null and parent has other children', () => {
        const recipe = { source: { config: { test: 'test', profiling: { enabled: true, testing: 'hello' } } } };
        const updatedRecipe = setFieldValueOnRecipe(recipe, null, 'source.config.profiling.testing');
        expect(updatedRecipe).toMatchObject({ source: { config: { test: 'test', profiling: { enabled: true } } } });
    });
});

describe('setListValuesOnRecipe', () => {
    const tableAllowFieldPath = 'source.config.table_pattern.allow';

    it('should update list values on a recipe when it was not defined', () => {
        const recipe = { source: { config: {} } };
        const updatedRecipe = setListValuesOnRecipe(recipe, ['*test_pattern'], tableAllowFieldPath);
        expect(updatedRecipe).toMatchObject({ source: { config: { table_pattern: { allow: ['*test_pattern'] } } } });
    });

    it('should update list values on a recipe when it was defined', () => {
        const recipe = { source: { config: { table_pattern: { allow: ['*test_pattern'] } } } };
        const updatedRecipe = setListValuesOnRecipe(recipe, ['*test_pattern(edit)'], tableAllowFieldPath);
        expect(updatedRecipe).toMatchObject({
            source: { config: { table_pattern: { allow: ['*test_pattern(edit)'] } } },
        });
    });

    it('should append list values on a recipe', () => {
        const recipe = { source: { config: { table_pattern: { allow: ['*test_pattern'] } } } };
        const updatedRecipe = setListValuesOnRecipe(recipe, ['*test_pattern', 'new'], tableAllowFieldPath);
        expect(updatedRecipe).toMatchObject({
            source: { config: { table_pattern: { allow: ['*test_pattern', 'new'] } } },
        });
    });

    it('should remove list values on a recipe', () => {
        const recipe = { source: { config: { table_pattern: { allow: ['*test_pattern', 'remove_me'] } } } };
        const updatedRecipe = setListValuesOnRecipe(recipe, ['*test_pattern'], tableAllowFieldPath);
        expect(updatedRecipe).toMatchObject({
            source: { config: { table_pattern: { allow: ['*test_pattern'] } } },
        });
    });

    it('should remove empty values from the list when updating a recipe', () => {
        const recipe = { source: { config: { table_pattern: { allow: ['*test_pattern'] } } } };
        const updatedRecipe = setListValuesOnRecipe(recipe, ['*test_pattern', '', '', ''], tableAllowFieldPath);
        expect(updatedRecipe).toMatchObject({
            source: { config: { table_pattern: { allow: ['*test_pattern'] } } },
        });
    });

    it('should clear the value and its parent when passing in empty list and parent has no other children', () => {
        const recipe = { source: { config: { existingField: true, table_pattern: { allow: ['*test_pattern'] } } } };
        const updatedRecipe = setListValuesOnRecipe(recipe, [], tableAllowFieldPath);
        expect(updatedRecipe).toMatchObject({ source: { config: { existingField: true } } });
    });

    it('should clear the value but not its parent when passing in empty list and parent has other children', () => {
        const recipe = { source: { config: { table_pattern: { allow: ['*test_pattern'], deny: ['test_deny'] } } } };
        const updatedRecipe = setListValuesOnRecipe(recipe, [], tableAllowFieldPath);
        expect(updatedRecipe).toMatchObject({ source: { config: { table_pattern: { deny: ['test_deny'] } } } });
    });
});
