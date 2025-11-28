import { renderHook } from '@testing-library/react-hooks';
import { describe, expect, it } from 'vitest';

import { useExtractMentions } from '@app/document/hooks/useExtractMentions';

describe('useExtractMentions', () => {
    it('should extract document URNs from markdown links', () => {
        const content = 'Check out [@Document 1](urn:li:document:abc123) for more info.';

        const { result } = renderHook(() => useExtractMentions(content));

        expect(result.current.documentUrns).toEqual(['urn:li:document:abc123']);
        expect(result.current.assetUrns).toEqual([]);
    });

    it('should extract asset URNs from markdown links', () => {
        const content = 'See [@Dataset](urn:li:dataset:xyz789) for the data.';

        const { result } = renderHook(() => useExtractMentions(content));

        expect(result.current.documentUrns).toEqual([]);
        expect(result.current.assetUrns).toEqual(['urn:li:dataset:xyz789']);
    });

    it('should extract multiple document and asset URNs', () => {
        const content = `
            Check [@Doc1](urn:li:document:doc1) and [@Doc2](urn:li:document:doc2).
            Also see [@Dataset1](urn:li:dataset:ds1) and [@Dataset2](urn:li:dataset:ds2).
        `;

        const { result } = renderHook(() => useExtractMentions(content));

        expect(result.current.documentUrns).toEqual(['urn:li:document:doc1', 'urn:li:document:doc2']);
        expect(result.current.assetUrns).toEqual(['urn:li:dataset:ds1', 'urn:li:dataset:ds2']);
    });

    it('should handle mixed document and asset URNs', () => {
        const content = `
            [@Document](urn:li:document:123)
            [@Dataset](urn:li:dataset:456)
            [@Chart](urn:li:chart:789)
            [@Another Doc](urn:li:document:abc)
        `;

        const { result } = renderHook(() => useExtractMentions(content));

        expect(result.current.documentUrns).toEqual(['urn:li:document:123', 'urn:li:document:abc']);
        expect(result.current.assetUrns).toEqual(['urn:li:dataset:456', 'urn:li:chart:789']);
    });

    it('should not extract duplicate URNs', () => {
        const content = `
            [@Doc](urn:li:document:123)
            [@Same Doc](urn:li:document:123)
            [@Dataset](urn:li:dataset:456)
            [@Same Dataset](urn:li:dataset:456)
        `;

        const { result } = renderHook(() => useExtractMentions(content));

        expect(result.current.documentUrns).toEqual(['urn:li:document:123']);
        expect(result.current.assetUrns).toEqual(['urn:li:dataset:456']);
    });

    it('should handle empty content', () => {
        const content = '';

        const { result } = renderHook(() => useExtractMentions(content));

        expect(result.current.documentUrns).toEqual([]);
        expect(result.current.assetUrns).toEqual([]);
    });

    it('should handle content without URNs', () => {
        const content = 'This is just plain text without any mentions.';

        const { result } = renderHook(() => useExtractMentions(content));

        expect(result.current.documentUrns).toEqual([]);
        expect(result.current.assetUrns).toEqual([]);
    });

    it('should handle markdown links without @ symbol', () => {
        const content = `
            [@Broken Link](not-a-urn)
            [No @ symbol](urn:li:document:123)
            Regular text
        `;

        const { result } = renderHook(() => useExtractMentions(content));

        // Note: The regex matches any markdown link with URN format, not just those with @
        expect(result.current.documentUrns).toEqual(['urn:li:document:123']);
        expect(result.current.assetUrns).toEqual([]);
    });

    it('should handle URNs with special characters', () => {
        const content = '[@Entity](urn:li:dataFlow:some-flow_123)';

        const { result } = renderHook(() => useExtractMentions(content));

        expect(result.current.documentUrns).toEqual([]);
        expect(result.current.assetUrns).toEqual(['urn:li:dataFlow:some-flow_123']);
    });

    it('should handle complex markdown with multiple entity types', () => {
        const content = `
            # Documentation
            
            See these resources:
            - [@User Guide](urn:li:document:guide-123)
            - [@API Docs](urn:li:document:api-456)
            - [@Dataset A](urn:li:dataset:dataset-a)
            - [@Dataset B](urn:li:dataset:dataset-b)
            - [@Dashboard](urn:li:dashboard:dash-1)
            - [@ML Model](urn:li:mlModel:model-xyz)
        `;

        const { result } = renderHook(() => useExtractMentions(content));

        expect(result.current.documentUrns).toEqual(['urn:li:document:guide-123', 'urn:li:document:api-456']);
        expect(result.current.assetUrns).toEqual([
            'urn:li:dataset:dataset-a',
            'urn:li:dataset:dataset-b',
            'urn:li:dashboard:dash-1',
            'urn:li:mlModel:model-xyz',
        ]);
    });

    it('should handle URNs in inline code blocks', () => {
        const content = 'Use `[@Doc](urn:li:document:123)` in your code.';

        const { result } = renderHook(() => useExtractMentions(content));

        expect(result.current.documentUrns).toEqual(['urn:li:document:123']);
    });

    it('should update when content changes', () => {
        const { result, rerender } = renderHook(({ content }) => useExtractMentions(content), {
            initialProps: { content: '[@Doc1](urn:li:document:1)' },
        });

        expect(result.current.documentUrns).toEqual(['urn:li:document:1']);

        rerender({ content: '[@Doc2](urn:li:document:2)' });

        expect(result.current.documentUrns).toEqual(['urn:li:document:2']);
    });

    it('should handle null or undefined content gracefully', () => {
        const { result: resultNull } = renderHook(() => useExtractMentions(null as any));
        expect(resultNull.current.documentUrns).toEqual([]);
        expect(resultNull.current.assetUrns).toEqual([]);

        const { result: resultUndefined } = renderHook(() => useExtractMentions(undefined as any));
        expect(resultUndefined.current.documentUrns).toEqual([]);
        expect(resultUndefined.current.assetUrns).toEqual([]);
    });

    it('should handle URNs with parentheses', () => {
        const content = '[@Complex Dataset](urn:li:dataset:(urn:li:dataPlatform:kafka,topic-123,PROD))';

        const { result } = renderHook(() => useExtractMentions(content));

        expect(result.current.documentUrns).toEqual([]);
        // The regex now correctly handles nested parentheses in URNs
        expect(result.current.assetUrns).toEqual(['urn:li:dataset:(urn:li:dataPlatform:kafka,topic-123,PROD)']);
    });
});
