import { downgradeV2FieldPath } from '@src/app/entityV2/dataset/profile/schema/utils/utils';

describe('downgradeV2FieldPath', () => {
    it('should handle simple field paths without modification', () => {
        expect(downgradeV2FieldPath('simple_field')).toBe('simple_field');
        expect(downgradeV2FieldPath('user_name')).toBe('user_name');
        expect(downgradeV2FieldPath('id')).toBe('id');
    });

    it('should handle nested field paths with dots', () => {
        expect(downgradeV2FieldPath('user.name')).toBe('user.name');
        expect(downgradeV2FieldPath('address.street.name')).toBe('address.street.name');
        expect(downgradeV2FieldPath('metadata.created.timestamp')).toBe('metadata.created.timestamp');
    });

    it('should remove array index segments starting with [', () => {
        expect(downgradeV2FieldPath('user.addresses[0].street')).toBe('user.addresses.street');
        expect(downgradeV2FieldPath('items[*].price')).toBe('items.price');
        expect(downgradeV2FieldPath('data[123].value')).toBe('data.value');
    });

    it('should handle complex nested paths with multiple brackets and dots', () => {
        expect(downgradeV2FieldPath('metadata.tags[0].category.name')).toBe('metadata.tags.category.name');
        expect(downgradeV2FieldPath('users[*].addresses[0].coordinates[1]')).toBe('users.addresses.coordinates');
        expect(downgradeV2FieldPath('data.records[0].attributes.values[*].name')).toBe(
            'data.records.attributes.values.name',
        );
    });

    it('should handle edge cases with consecutive brackets', () => {
        expect(downgradeV2FieldPath('data[0][1].value')).toBe('data.value');
        expect(downgradeV2FieldPath('matrix[*][*].cell')).toBe('matrix.cell');
    });

    it('should handle field paths starting with brackets', () => {
        expect(downgradeV2FieldPath('[0].value')).toBe('value');
        expect(downgradeV2FieldPath('[*].item.name')).toBe('item.name');
    });

    it('should handle field paths ending with brackets', () => {
        expect(downgradeV2FieldPath('data.items[0]')).toBe('data.items');
        expect(downgradeV2FieldPath('users.addresses[*]')).toBe('users.addresses');
    });

    it('should handle null and undefined inputs', () => {
        expect(downgradeV2FieldPath(null)).toBe(null);
        expect(downgradeV2FieldPath(undefined)).toBe(undefined);
    });

    it('should handle empty string', () => {
        expect(downgradeV2FieldPath('')).toBe('');
    });

    it('should handle field paths with only brackets', () => {
        expect(downgradeV2FieldPath('[0]')).toBe('');
        expect(downgradeV2FieldPath('[*]')).toBe('');
        expect(downgradeV2FieldPath('[0][1][2]')).toBe('');
    });

    it('should handle mixed bracket formats', () => {
        expect(downgradeV2FieldPath('data[0].items[*].tags[type].value')).toBe('data.items.tags.value');
        expect(downgradeV2FieldPath('users[id=123].profile.settings[0]')).toBe('users.profile.settings');
    });

    it('should preserve field names that contain bracket-like characters but are not array indices', () => {
        // This test ensures that field names themselves containing brackets are preserved
        // if they don't start with [ (though this is an edge case)
        expect(downgradeV2FieldPath('field_with_brackets].value')).toBe('field_with_brackets].value');
    });

    // Test cases that specifically relate to our nested column drawer fix
    describe('Nested Column Drawer Fix Scenarios', () => {
        it('should handle typical nested object structures', () => {
            expect(downgradeV2FieldPath('customer.address.street')).toBe('customer.address.street');
            expect(downgradeV2FieldPath('order.items[0].product.name')).toBe('order.items.product.name');
        });

        it('should handle array of primitives', () => {
            expect(downgradeV2FieldPath('tags[*]')).toBe('tags');
            expect(downgradeV2FieldPath('categories[0]')).toBe('categories');
        });

        it('should handle deeply nested array structures', () => {
            expect(downgradeV2FieldPath('data.levels[0].sublevels[*].items[0].value')).toBe(
                'data.levels.sublevels.items.value',
            );
        });

        it('should handle JSON-like structures common in datasets', () => {
            expect(downgradeV2FieldPath('event.properties.custom_fields[user_id]')).toBe(
                'event.properties.custom_fields',
            );
            expect(downgradeV2FieldPath('response.data.results[0].metadata.tags[*].name')).toBe(
                'response.data.results.metadata.tags.name',
            );
        });
    });
});
