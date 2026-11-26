import { describe, expect, it } from 'vitest';

import { resolveDisplayIconName } from '@app/sharedV2/icons/resolveDisplayIcon';

import { IconLibrary } from '@types';

describe('resolveDisplayIconName', () => {
    it('maps Material library icons', () => {
        expect(resolveDisplayIconName('Bookmark', IconLibrary.Material)).toBe('BookmarkSimple');
        expect(resolveDisplayIconName('AccountCircle', IconLibrary.Material)).toBe('UserCircle');
    });

    it('passes Phosphor library icons through without Material remapping', () => {
        expect(resolveDisplayIconName('Bookmark', IconLibrary.Phosphor)).toBe('Bookmark');
        expect(resolveDisplayIconName('UserCircle', IconLibrary.Phosphor)).toBe('UserCircle');
    });

    it('treats a missing library as Material for legacy records', () => {
        expect(resolveDisplayIconName('Home')).toBe('House');
    });
});
