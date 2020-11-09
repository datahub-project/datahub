import chai = require('chai');
import {
  buildIgnoreRegExp,
  formatDocs,
  makeRelative,
  packageToNamespace,
  stringConstant,
  typeToNamespace
} from '../../src/ts-emitter';

const assert = chai.assert;

describe('formatDocs', () => {
  it('adds // to simple string', () => {
    const doc = formatDocs('abc');
    assert.equal(doc, '// abc');
  });

  it('adds // per line', () => {
    const doc = formatDocs('abc\n');
    assert.equal(doc, '// abc\n//');
  });

  it('does not duplicate space', () => {
    const doc = formatDocs(' abc');
    assert.equal(doc, '// abc');
  });

  it('uses prefix', () => {
    const doc = formatDocs(' abc', '   ');
    assert.equal(doc, '   // abc');
  });
});

describe('stringConstant', () => {
  it('simple string', () => {
    const s = stringConstant('abc');
    assert.equal("'abc'", s);
  });

  it('quotes single quote', () => {
    const s = stringConstant("a'bc");
    assert.equal("'a\\'bc'", s);
  });

  it('squashes new lines', () => {
    const s = stringConstant('a\nbc');
    assert.equal("'a bc'", s);
  });
});

describe('makeRelative`', () => {
  it('removes / in first position', () => {
    const s = makeRelative('/abc');
    assert.equal('abc', s);
  });

  it('passes through relative', () => {
    const s = makeRelative('ab/c');
    assert.equal('ab/c', s);
  });
});

describe('packageToNamespace', () => {
  it('capitalizes packages', () => {
    const ns = packageToNamespace('com.linkedin.one', buildIgnoreRegExp(['com.']));
    assert.equal('Linkedin.One', ns.join('.'));
  });
  it('capitalizes packages without ignore', () => {
    const ns = packageToNamespace('com.linkedin.one', undefined);
    assert.equal('Com.Linkedin.One', ns.join('.'));
  });
});

describe('typeToNamespace', () => {
  it('capitalizes the packages for the type', () => {
    const ns = typeToNamespace('com.linkedin.one', buildIgnoreRegExp(['com.']));
    assert.equal('Linkedin.one', ns);
  });
});
