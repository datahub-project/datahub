import { jsonEscapeStr } from '../textUtil';

describe('jsonEscapeStr', () => {
    it('should escape newline tab and carriage return', () => {
        expect(jsonEscapeStr('\n')).toStrictEqual('\\n');
        expect(jsonEscapeStr('my new \n string')).toStrictEqual('my new \\n string');
        expect(jsonEscapeStr('\r')).toStrictEqual('\\r');
        expect(jsonEscapeStr('my new \r string')).toStrictEqual('my new \\r string');
        expect(jsonEscapeStr('\t')).toStrictEqual('\\t');
        expect(jsonEscapeStr('my new \t string')).toStrictEqual('my new \\t string');
    });
});
