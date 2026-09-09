import { describe, expect, it } from 'vitest';

import { resolveRemirrorLocale } from '@components/components/Editor/RemirrorLocaleProvider';

describe('resolveRemirrorLocale', () => {
    it('prefers an exact BCP-47 match when a bundle exists', () => {
        expect(resolveRemirrorLocale('zh-CN')).toBe('zh-CN');
        expect(resolveRemirrorLocale('pt-BR')).toBe('pt-BR');
    });

    it('falls back to the primary language subtag when only that bundle exists', () => {
        expect(resolveRemirrorLocale('de-DE')).toBe('de');
        expect(resolveRemirrorLocale('ja-JP')).toBe('ja');
    });

    it('falls back to English when no bundle matches', () => {
        expect(resolveRemirrorLocale('pt-PT')).toBe('en');
        expect(resolveRemirrorLocale('zh-Hant-TW')).toBe('en');
        expect(resolveRemirrorLocale('ko-KR')).toBe('en');
    });
});
