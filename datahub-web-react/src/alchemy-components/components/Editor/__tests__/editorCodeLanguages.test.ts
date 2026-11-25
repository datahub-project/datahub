import refractor from 'refractor/core';
import { describe, expect, it } from 'vitest';

import {
    listEditorCodeLanguages,
    registerEditorCodeLanguages,
} from '@components/components/Editor/editorCodeLanguages';

describe('editor code languages', () => {
    it('registers the full catalog, including java, scala, and go', () => {
        registerEditorCodeLanguages();
        const languages = listEditorCodeLanguages();

        expect(languages).toEqual(expect.arrayContaining(['java', 'scala', 'go', 'sql', 'python']));
        expect(refractor.registered('java')).toBe(true);
        expect(refractor.registered('scala')).toBe(true);
        expect(refractor.registered('go')).toBe(true);
    });
});
