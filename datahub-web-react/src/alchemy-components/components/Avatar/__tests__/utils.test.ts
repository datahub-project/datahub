import { afterEach, describe, expect, it, vi } from 'vitest';

import { getAvatarPillBorderStyle, getAvatarSizes, getNameInitials } from '@components/components/Avatar/utils';

import { colors } from '@src/alchemy-components/theme';

describe('Avatar utils', () => {
    afterEach(() => {
        vi.clearAllMocks();
    });

    describe('get initials of the name', () => {
        it('get initials of name with first name and last name', () => {
            expect(getNameInitials('John Doe ')).toEqual('JD');
        });
        it('get initials of name with first name and last name in lower case', () => {
            expect(getNameInitials('john doe')).toEqual('JD');
        });
        it('get initials of name with only first name', () => {
            expect(getNameInitials('Robert')).toEqual('RO');
        });
        it('get initials of name with only first name in lower case', () => {
            expect(getNameInitials('robert')).toEqual('RO');
        });
        it('get initials of name with three names', () => {
            expect(getNameInitials('James Edward Brown')).toEqual('JB');
        });
        it('get initials of name with four names', () => {
            expect(getNameInitials('Michael James Alexander Scott')).toEqual('MS');
        });
        it('get initials of name with a hyphen', () => {
            expect(getNameInitials('Mary-Jane Watson')).toEqual('MW');
        });
        it('get initials of name with an apostrophe', () => {
            expect(getNameInitials("O'Connor")).toEqual('OC');
        });
        it('get initials of name with a single letter', () => {
            expect(getNameInitials('J')).toEqual('J');
        });
        it('get initials of name with an empty string', () => {
            expect(getNameInitials('')).toEqual('');
        });
    });

    describe('getAvatarPillBorderStyle', () => {
        it('should return dashed border style', () => {
            const borderStyle = getAvatarPillBorderStyle('dashed');
            expect(borderStyle).toBe(`1px dashed ${colors.gray[100]}`);
        });

        it('should return solid border style for undefined', () => {
            const borderStyle = getAvatarPillBorderStyle(undefined);
            expect(borderStyle).toBe(`1px solid ${colors.gray[100]}`);
        });

        it('should return solid border style for other cases', () => {
            const borderStyle = getAvatarPillBorderStyle('default');
            expect(borderStyle).toBe(`1px solid ${colors.gray[100]}`);
        });
    });

    describe('getAvatarSizes', () => {
        it('should return correct sizes for xs', () => {
            const sizes = getAvatarSizes('xs');
            expect(sizes).toEqual({ width: '16px', height: '16px', fontSize: '7px' });
        });

        it('should return correct sizes for sm', () => {
            const sizes = getAvatarSizes('sm');
            expect(sizes).toEqual({ width: '18px', height: '18px', fontSize: '8px' });
        });

        it('should return correct sizes for md', () => {
            const sizes = getAvatarSizes('md');
            expect(sizes).toEqual({ width: '24px', height: '24px', fontSize: '12px' });
        });

        it('should return correct sizes for lg', () => {
            const sizes = getAvatarSizes('lg');
            expect(sizes).toEqual({ width: '28px', height: '28px', fontSize: '14px' });
        });

        it('should return correct sizes for xl', () => {
            const sizes = getAvatarSizes('xl');
            expect(sizes).toEqual({ width: '32px', height: '32px', fontSize: '14px' });
        });

        it('should return default sizes', () => {
            const sizes = getAvatarSizes('default');
            expect(sizes).toEqual({ width: '20px', height: '20px', fontSize: '10px' });
        });
    });
});
