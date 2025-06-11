import { mergeConfig } from '@app/connections/utils';

// adjust path as needed

describe('mergeConfig', () => {
    it('merges flat keys from both config and formValues', () => {
        const config = { a: 1, b: 2 };
        const formValues = { b: 3, c: 4 };

        const result = mergeConfig(config, formValues);
        expect(result).toEqual({ a: 1, b: 3, c: 4 });
    });

    it('merges nested objects recursively', () => {
        const config = { a: { x: 1, y: 2 }, b: 2 };
        const formValues = { a: { y: 3, z: 4 } };

        const result = mergeConfig(config, formValues);
        expect(result).toEqual({ a: { x: 1, y: 3, z: 4 }, b: 2 });
    });

    it('uses formValues when config is missing key', () => {
        const config = {};
        const formValues = { a: 42 };

        const result = mergeConfig(config, formValues);
        expect(result).toEqual({ a: 42 });
    });

    it('uses config values when formValues are missing', () => {
        const config = { a: 1, b: 2 };
        const formValues = {};

        const result = mergeConfig(config, formValues);
        expect(result).toEqual({ a: 1, b: 2 });
    });

    it('replaces non-object values without deep merge', () => {
        const config = { a: { x: 1 }, b: 2 };
        const formValues = { a: 42 };

        const result = mergeConfig(config, formValues);
        expect(result).toEqual({ a: 42, b: 2 });
    });
});
