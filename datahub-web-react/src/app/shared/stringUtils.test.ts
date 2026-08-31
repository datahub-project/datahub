import { convertToCamelCase, convertToKebabCase, convertToPascalCase } from '@app/shared/stringUtils';

describe('stringUtils', () => {
    describe('convertToPascalCase', () => {
        it('converts kebab-case to PascalCase', () => {
            expect(convertToPascalCase('magnifying-glass')).toBe('MagnifyingGlass');
            expect(convertToPascalCase('user-profile')).toBe('UserProfile');
            expect(convertToPascalCase('api-key-manager')).toBe('ApiKeyManager');
        });

        it('handles single word', () => {
            expect(convertToPascalCase('user')).toBe('User');
            expect(convertToPascalCase('profile')).toBe('Profile');
        });

        it('handles already capitalized words', () => {
            expect(convertToPascalCase('User-Profile')).toBe('UserProfile');
        });

        it('handles empty string', () => {
            expect(convertToPascalCase('')).toBe('');
        });
    });

    describe('convertToCamelCase', () => {
        it('converts kebab-case to camelCase', () => {
            expect(convertToCamelCase('magnifying-glass')).toBe('magnifyingGlass');
            expect(convertToCamelCase('user-profile')).toBe('userProfile');
            expect(convertToCamelCase('api-key-manager')).toBe('apiKeyManager');
        });

        it('handles single word', () => {
            expect(convertToCamelCase('user')).toBe('user');
            expect(convertToCamelCase('profile')).toBe('profile');
        });

        it('handles empty string', () => {
            expect(convertToCamelCase('')).toBe('');
        });
    });

    describe('convertToKebabCase', () => {
        it('converts PascalCase to kebab-case', () => {
            expect(convertToKebabCase('MagnifyingGlass')).toBe('magnifying-glass');
            expect(convertToKebabCase('UserProfile')).toBe('user-profile');
            expect(convertToKebabCase('APIKeyManager')).toBe('api-key-manager');
        });

        it('converts camelCase to kebab-case', () => {
            expect(convertToKebabCase('magnifyingGlass')).toBe('magnifying-glass');
            expect(convertToKebabCase('userProfile')).toBe('user-profile');
            expect(convertToKebabCase('apiKeyManager')).toBe('api-key-manager');
        });

        it('handles single word', () => {
            expect(convertToKebabCase('User')).toBe('user');
            expect(convertToKebabCase('user')).toBe('user');
        });

        it('handles empty string', () => {
            expect(convertToKebabCase('')).toBe('');
        });
    });
});
