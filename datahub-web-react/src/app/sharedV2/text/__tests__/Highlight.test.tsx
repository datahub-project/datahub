import { render } from '@testing-library/react';
import React from 'react';

import Highlight from '@app/sharedV2/text/Highlight';

function marks(container: HTMLElement): string[] {
    return Array.from(container.querySelectorAll('mark')).map((mark) => mark.textContent ?? '');
}

describe('Highlight', () => {
    it('highlights every case-insensitive match and keeps the full text', () => {
        const { container } = render(<Highlight search="foo">Foo bar foo</Highlight>);
        expect(marks(container)).toEqual(['Foo', 'foo']);
        expect(container.textContent).toBe('Foo bar foo');
    });

    it('treats string search literally, not as a regex', () => {
        const { container } = render(<Highlight search="a.b(">xa.b(y axb</Highlight>);
        expect(marks(container)).toEqual(['a.b(']);
    });

    it('supports regex search, including a match-everything pattern', () => {
        const { container } = render(<Highlight search={/.*/}>whole text</Highlight>);
        expect(marks(container)).toEqual(['whole text']);
    });

    it('respects caseSensitive', () => {
        const { container } = render(
            <Highlight search="Foo" caseSensitive>
                foo Foo
            </Highlight>,
        );
        expect(marks(container)).toEqual(['Foo']);
    });

    it('highlights every match of a global regex', () => {
        const { container } = render(<Highlight search={/foo/g}>foo bar foo</Highlight>);
        expect(marks(container)).toEqual(['foo', 'foo']);
    });

    it('evaluates anchors against the full text and skips empty matches', () => {
        const { container: anchored } = render(<Highlight search={/^foo/}>foofoo</Highlight>);
        const { container: optional } = render(<Highlight search={/x*/}>axb</Highlight>);
        expect(marks(anchored)).toEqual(['foo']);
        expect(marks(optional)).toEqual(['x']);
        expect(optional.textContent).toBe('axb');
    });

    it('renders text unchanged when search is empty or there is no match', () => {
        const { container: empty } = render(<Highlight search="">text</Highlight>);
        const { container: noMatch } = render(<Highlight search="zzz">text</Highlight>);
        expect(marks(empty)).toEqual([]);
        expect(marks(noMatch)).toEqual([]);
        expect(noMatch.textContent).toBe('text');
    });

    it('applies match styling and passes span props through', () => {
        const { container } = render(
            <Highlight search="b" matchStyle={{ fontWeight: 700 }} className="custom">
                abc
            </Highlight>,
        );
        const mark = container.querySelector('mark');
        expect(mark).toHaveClass('highlight');
        expect(mark).toHaveStyle({ fontWeight: '700' });
        expect(container.firstElementChild).toHaveClass('custom');
    });
});
