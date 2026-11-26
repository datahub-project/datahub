import { dedupeByUrn } from '@utils/dedupeByUrn';

type Item = { urn: string; propagated?: boolean; label?: string };

const getUrn = (item: Item) => item.urn;
const isPropagated = (item: Item) => !!item.propagated;

describe('dedupeByUrn', () => {
    it('removes duplicate urns and preserves first-occurrence order', () => {
        const items: Item[] = [{ urn: 'a' }, { urn: 'b' }, { urn: 'a' }, { urn: 'c' }];
        expect(dedupeByUrn(items, getUrn).map(getUrn)).toEqual(['a', 'b', 'c']);
    });

    it('prefers the non-propagated entry regardless of order', () => {
        const propagatedFirst: Item[] = [
            { urn: 'a', propagated: true, label: 'auto' },
            { urn: 'a', propagated: false, label: 'manual' },
        ];
        expect(dedupeByUrn(propagatedFirst, getUrn, isPropagated)[0].label).toBe('manual');

        const manualFirst: Item[] = [
            { urn: 'a', propagated: false, label: 'manual' },
            { urn: 'a', propagated: true, label: 'auto' },
        ];
        expect(dedupeByUrn(manualFirst, getUrn, isPropagated)[0].label).toBe('manual');
    });

    it('keeps the first occurrence when propagation status ties', () => {
        const items: Item[] = [
            { urn: 'a', propagated: true, label: 'first' },
            { urn: 'a', propagated: true, label: 'second' },
        ];
        expect(dedupeByUrn(items, getUrn, isPropagated)[0].label).toBe('first');
    });

    it('keeps an earlier non-propagated entry over a later duplicate (priority via list order)', () => {
        // Callers put higher-priority (e.g. non-removable) entries first; an earlier non-propagated entry
        // must win so a duplicate is never shown as removable.
        const items: Item[] = [
            { urn: 'a', propagated: false, label: 'uneditable' },
            { urn: 'a', propagated: false, label: 'editable' },
        ];
        expect(dedupeByUrn(items, getUrn, isPropagated)[0].label).toBe('uneditable');
    });
});
