import { getLinkPrefix, getLinkWithoutPrefix } from '@app/settingsV2/helpLink/LinkInput';

describe('link prefix regex', () => {
    it('should get the link prefix properly with https://', () => {
        const linkPrefix = getLinkPrefix('https://www.google.com');
        expect(linkPrefix).toBe('https://');
    });

    it('should get the link prefix properly with http://', () => {
        const linkPrefix = getLinkPrefix('http://www.google.com');
        expect(linkPrefix).toBe('http://');
    });

    it('should get the link prefix properly with mailto:', () => {
        const linkPrefix = getLinkPrefix('mailto:test@gmail.com');
        expect(linkPrefix).toBe('mailto:');
    });

    it('should get the link without the prefix properly with https://', () => {
        const linkPrefix = getLinkWithoutPrefix('https://www.google.com');
        expect(linkPrefix).toBe('www.google.com');
    });

    it('should get the link without the prefix properly with http://', () => {
        const linkPrefix = getLinkWithoutPrefix('http://www.google.com');
        expect(linkPrefix).toBe('www.google.com');
    });

    it('should get the link without the prefix properly with mailto:', () => {
        const linkPrefix = getLinkWithoutPrefix('mailto:test@gmail.com');
        expect(linkPrefix).toBe('test@gmail.com');
    });
});
