import { markdownToHtml } from '../../../../components/editor/extensions/markdownToHtml';

const cases = [
    [
        'should parse datahub mentions',
        'Lorem [@SampleHiveDataset](urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)) ipsum',
        '<p>Lorem <span data-datahub-mention-urn="urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)">@SampleHiveDataset</span> ipsum</p>\n',
    ],
    ['should not parse github mentions', 'Lorem @githubuser ipsum', '<p>Lorem @githubuser ipsum</p>\n'],
    [
        'should parse invalid mentions as links',
        'Lorem [@Some link](/lorem-ipsum) ipsum',
        '<p>Lorem <a href="/lorem-ipsum">@Some link</a> ipsum</p>\n',
    ],
];

describe('markdownToHtml', () => {
    it.each(cases)('%s', (_, input, expected) => {
        expect(markdownToHtml(input)).toBe(expected);
    });
});
