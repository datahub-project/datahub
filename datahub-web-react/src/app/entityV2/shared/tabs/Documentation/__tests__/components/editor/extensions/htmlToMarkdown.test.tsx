import { htmlToMarkdown } from '../../../../components/editor/extensions/htmlToMarkdown';

const cases = [
    ['strike', '<strike>Lorem ipsum</strike>', '~Lorem ipsum~'],
    ['s', '<s>Lorem ipsum</s>', '~Lorem ipsum~'],
    ['del', '<del>Lorem ipsum</del>', '~Lorem ipsum~'],
    [
        'should replace blank nodes in table',
        '<table><thead><tr><th>Lorem ipsum</th></tr></thead><tbody><tr><td><p>Lorem</p><p>ipsum</p></td></tr></tbody></table>',
        '| Lorem ipsum |\n| --- |\n| Lorem<br />ipsum |',
    ],
    ['should replace empty p tags to line breaks', '<p>Lorem</p><p></p><p>ipsum</p>', 'Lorem\n\n&nbsp;\n\nipsum'],
    [
        'should parse image if it does not have a width attribute',
        '<img src="/my-image.png" style="width: 100%; min-width: 50px; object-fit: contain;" alt="my image">',
        '![my image](/my-image.png)',
    ],

    [
        'should highlight code block with html',
        '<pre><code class="language language-html">&lt;<span class="pl-ent">p</span>&gt;Hello world&lt;/<span class="pl-ent">p</span>&gt;</code></pre>',
        '```html\n<p>Hello world</p>\n```',
    ],
    [
        'should highlight code block with js',
        '<pre><code class="lang lang-js">;(<span class="pl-k">function</span> () {})()</code></pre>',
        '```js\n;(function () {})()\n```',
    ],
    [
        'should highlighted remirror code block',
        '<pre><code data-code-block-language="ts">;(<span class="pl-k">function</span> () {})()</code></pre>',
        '```ts\n;(function () {})()\n```',
    ],
    [
        'should parse without language code block',
        '<pre><code>;(<span class="pl-k">function</span> () {})()</code></pre>',
        '```\n;(function () {})()\n```',
    ],
    [
        'should parse datahub mention',
        '<span class="mentions" data-datahub-mention-urn="urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)">@SampleHiveDataset</span>',
        '[@SampleHiveDataset](urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD))',
    ],
];

const skipParseCases = [
    [
        'table if ul or li is present',
        '<table><thead><tr><th>Lorem ipsum</th></tr></thead><tbody><tr><td><ul><li>Lorem</li><li>ipsum</li></ul></td></tr></tbody></table>',
    ],
    [
        'table if  present',
        '<table><thead><tr><th>Lorem ipsum</th></tr></thead><tbody><tr><td><ul><li>Lorem</li><li>ipsum</li></ul></td></tr></tbody></table>',
    ],
    [
        'image if it has a width attribute',
        '<img src="/my-image.png" style="width: 100%; min-width: 50px; object-fit: contain;" alt="my image" width="200">',
    ],
];

describe('htmlToMarkdown', () => {
    it.each(cases)('%s', (_, input, expected) => {
        expect(htmlToMarkdown(input)).toBe(expected);
    });

    it.each(skipParseCases)('should skip parsing %s', (_, input) => {
        expect(htmlToMarkdown(input)).toBe(input);
    });
});
