import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { measuredSampleNote } from './sample-size.mjs';

describe('measuredSampleNote', () => {
    it('states the achieved count when every configured run succeeded', () => {
        const note = measuredSampleNote({ A: 5, B: 5 }, 5);
        assert.equal(note.line, 'Measured runs in the table: A 5/5, B 5/5.');
        assert.equal(note.warning, null);
    });

    it('warns when a build contributed fewer runs than the target', () => {
        const note = measuredSampleNote({ A: 3, B: 5 }, 5);
        assert.match(note.line, /A 3\/5, B 5\/5/);
        assert.match(note.warning, /Short of the configured run count/);
    });
});
