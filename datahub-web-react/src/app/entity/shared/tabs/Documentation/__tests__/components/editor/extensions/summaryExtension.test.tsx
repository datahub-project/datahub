import { SummaryExtension } from '../../../../components/editor/extensions/SummaryExtension';
import {
    ApplySchemaAttributes,
    ExtensionTag,
    MarkSpecOverride,
} from '@remirror/core';


describe('SummaryExtension', () => {
    const summaryExtension = new SummaryExtension();
    it('name', () => {
        expect(summaryExtension.name).toEqual('summary');
    });
    it('tags', () => {
        expect(summaryExtension.createTags()).toEqual([ExtensionTag.Block, ExtensionTag.LastNodeCompatible]);
    });
    it('createMarkSpec', () => {
        const extra = { defaults: () => {}, dom: node => node } as ApplySchemaAttributes;
        const override = { parseDOM: ['abc'] } as MarkSpecOverride;

        const spec = summaryExtension.createNodeSpec(extra, override);

        expect(spec.content).toEqual('inline*');
        expect(spec.draggable).toEqual(false);
        expect(spec.attrs).toEqual(extra.defaults());
        expect(spec.toDOM({})).toEqual( ['summary', extra.dom({}), 0]);
    });
});
