import { safeUrl } from '@app/shared/urlUtils';

describe('safeUrl', () => {
    it('allows http URLs', () => {
        expect(safeUrl('http://example.com')).toBe('http://example.com');
    });

    it('allows https URLs', () => {
        expect(safeUrl('https://example.com/path?q=1')).toBe('https://example.com/path?q=1');
    });

    it('allows ftp URLs', () => {
        expect(safeUrl('ftp://files.example.com/file.txt')).toBe('ftp://files.example.com/file.txt');
    });

    it('allows mailto URLs', () => {
        expect(safeUrl('mailto:user@example.com')).toBe('mailto:user@example.com');
    });

    it('blocks javascript: scheme', () => {
        // eslint-disable-next-line no-script-url
        expect(safeUrl('javascript:alert(1)')).toBe('about:blank');
    });

    it('blocks javascript: scheme case-insensitively', () => {
        // eslint-disable-next-line no-script-url
        expect(safeUrl('JAVASCRIPT:alert(document.cookie)')).toBe('about:blank');
    });

    it('blocks data: scheme', () => {
        expect(safeUrl('data:text/html,<script>alert(1)</script>')).toBe('about:blank');
    });

    it('blocks data: base64 scheme', () => {
        expect(safeUrl('data:text/html;base64,PHNjcmlwdD5hbGVydCgxKTwvc2NyaXB0Pg==')).toBe('about:blank');
    });

    it('blocks vbscript: scheme', () => {
        expect(safeUrl('vbscript:MsgBox(1)')).toBe('about:blank');
    });

    it('returns empty string for null', () => {
        expect(safeUrl(null)).toBe('');
    });

    it('returns empty string for undefined', () => {
        expect(safeUrl(undefined)).toBe('');
    });

    it('returns relative paths unchanged', () => {
        expect(safeUrl('/some/path')).toBe('/some/path');
    });
});
