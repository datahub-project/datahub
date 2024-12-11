import { getNameInitials } from '../utils';

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
