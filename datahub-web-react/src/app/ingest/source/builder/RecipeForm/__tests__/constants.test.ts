import { setFieldValueOnRecipe, setListValuesOnRecipe } from '../constants';

describe('setFieldValueOnRecipe', () => {
    it('should set the field value on a recipe object when it was not defined', () => {
        const recipe = { source: { config: {} } };
        const updatedRecipe = setFieldValueOnRecipe(recipe, 'test', 'account_id');
        expect(updatedRecipe).toMatchObject({ source: { config: { account_id: 'test' } } });
    });

    it('should update the field value on a recipe object when it was defined', () => {
        const recipe = { source: { config: { account_id: 'test' } } };
        const updatedRecipe = setFieldValueOnRecipe(recipe, 'edited!', 'account_id');
        expect(updatedRecipe).toMatchObject({ source: { config: { account_id: 'edited!' } } });
    });

    it('should update the field value on a recipe without changing any other fields', () => {
        const recipe = { source: { config: { test: 'test' } } };
        const updatedRecipe = setFieldValueOnRecipe(recipe, 'edited!', 'account_id');
        expect(updatedRecipe).toMatchObject({ source: { config: { test: 'test', account_id: 'edited!' } } });
    });

    it('should clear the key: value pair when passing in null', () => {
        const recipe = { source: { config: { account_id: 'test' } } };
        const updatedRecipe = setFieldValueOnRecipe(recipe, null, 'account_id');
        expect(updatedRecipe).toMatchObject({ source: { config: {} } });
    });

    it('should return the original recipe when passing in undefined', () => {
        const recipe = { source: { config: { test: 'test' } } };
        const updatedRecipe = setFieldValueOnRecipe(recipe, undefined, 'account_id');
        expect(updatedRecipe).toMatchObject(recipe);
    });

    it('should set the field value on a recipe object when it was not defined and has a parent', () => {
        const recipe = { source: { config: {} } };
        const updatedRecipe = setFieldValueOnRecipe(recipe, true, 'enabled', 'profiling');
        expect(updatedRecipe).toMatchObject({ source: { config: { profiling: { enabled: true } } } });
    });

    it('should update the field value on a recipe object when it was defined and has a parent', () => {
        const recipe = { source: { config: { profiling: { enabled: true } } } };
        const updatedRecipe = setFieldValueOnRecipe(recipe, false, 'enabled', 'profiling');
        expect(updatedRecipe).toMatchObject({ source: { config: { profiling: { enabled: false } } } });
    });

    it('should update the field value with a parent on a recipe without changing any other fields', () => {
        const recipe = { source: { config: { test: 'test' } } };
        const updatedRecipe = setFieldValueOnRecipe(recipe, false, 'enabled', 'profiling');
        expect(updatedRecipe).toMatchObject({ source: { config: { test: 'test', profiling: { enabled: false } } } });
    });

    it('should clear the field and its parent when passing in null and field is only child of parent', () => {
        const recipe = { source: { config: { test: 'test', profiling: { enabled: true } } } };
        const updatedRecipe = setFieldValueOnRecipe(recipe, null, 'enabled', 'profiling');
        expect(updatedRecipe).toMatchObject({ source: { config: { test: 'test' } } });
    });

    it('should clear the field but not its parent when passing in null and parent has other children', () => {
        const recipe = { source: { config: { test: 'test', profiling: { enabled: true, testing: 'hello' } } } };
        const updatedRecipe = setFieldValueOnRecipe(recipe, null, 'testing', 'profiling');
        expect(updatedRecipe).toMatchObject({ source: { config: { test: 'test', profiling: { enabled: true } } } });
    });
});

describe('setListValuesOnRecipe', () => {
    it('should update list values on a recipe when it was not defined', () => {
        const recipe = { source: { config: {} } };
        const updatedRecipe = setListValuesOnRecipe(recipe, ['*test_pattern'], 'allow', 'table_pattern');
        expect(updatedRecipe).toMatchObject({ source: { config: { table_pattern: { allow: ['*test_pattern'] } } } });
    });

    it('should update list values on a recipe when it was defined', () => {
        const recipe = { source: { config: { table_pattern: { allow: ['*test_pattern'] } } } };
        const updatedRecipe = setListValuesOnRecipe(recipe, ['*test_pattern(edit)'], 'allow', 'table_pattern');
        expect(updatedRecipe).toMatchObject({
            source: { config: { table_pattern: { allow: ['*test_pattern(edit)'] } } },
        });
    });

    it('should append list values on a recipe', () => {
        const recipe = { source: { config: { table_pattern: { allow: ['*test_pattern'] } } } };
        const updatedRecipe = setListValuesOnRecipe(recipe, ['*test_pattern', 'new'], 'allow', 'table_pattern');
        expect(updatedRecipe).toMatchObject({
            source: { config: { table_pattern: { allow: ['*test_pattern', 'new'] } } },
        });
    });

    it('should remove list values on a recipe', () => {
        const recipe = { source: { config: { table_pattern: { allow: ['*test_pattern', 'remove_me'] } } } };
        const updatedRecipe = setListValuesOnRecipe(recipe, ['*test_pattern'], 'allow', 'table_pattern');
        expect(updatedRecipe).toMatchObject({
            source: { config: { table_pattern: { allow: ['*test_pattern'] } } },
        });
    });

    it('should remove empty values from the list when updating a recipe', () => {
        const recipe = { source: { config: { table_pattern: { allow: ['*test_pattern'] } } } };
        const updatedRecipe = setListValuesOnRecipe(recipe, ['*test_pattern', '', '', ''], 'allow', 'table_pattern');
        expect(updatedRecipe).toMatchObject({
            source: { config: { table_pattern: { allow: ['*test_pattern'] } } },
        });
    });

    it('should clear the value and its parent when passing in empty list and parent has no other children', () => {
        const recipe = { source: { config: { table_pattern: { allow: ['*test_pattern'] } } } };
        const updatedRecipe = setListValuesOnRecipe(recipe, [], 'allow', 'table_pattern');
        expect(updatedRecipe).toMatchObject({ source: { config: {} } });
    });

    it('should clear the value but not its parent when passing in empty list and parent has other children', () => {
        const recipe = { source: { config: { table_pattern: { allow: ['*test_pattern'], deny: ['test_deny'] } } } };
        const updatedRecipe = setListValuesOnRecipe(recipe, [], 'allow', 'table_pattern');
        expect(updatedRecipe).toMatchObject({ source: { config: { table_pattern: { deny: ['test_deny'] } } } });
    });
});
