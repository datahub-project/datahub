import { DetailsExtension } from '../../../../components/editor/extensions/detailsExtension';
import {
    ApplySchemaAttributes,
    ExtensionTag,
    MarkSpecOverride,
} from '@remirror/core';

describe('DetailsExtension', () => {
    const detailsExtension = new DetailsExtension();
    it('name', () => {
        expect(detailsExtension.name).toEqual('details');
    });
    it('tags', () => {
        expect(detailsExtension.createTags()).toEqual([ExtensionTag.Block]);
    });
    it('createMarkSpec', () => {
        const extra = { defaults: () => {}, dom: node => node } as ApplySchemaAttributes;
        const override = { parseDOM: ['abc'] } as MarkSpecOverride;

        const spec = detailsExtension.createNodeSpec(extra, override);

        expect(spec.content).toEqual('block*');
        expect(spec.draggable).toEqual(false);
        expect(spec.attrs).toEqual(extra.defaults());
        expect(spec.toDOM({})).toEqual( ['details', extra.dom({}), 0]);
    });
});
