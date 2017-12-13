import * as complianceDataTypesApi from 'wherehows-web/utils/api/list/compliance-datatypes';
import { module, test } from 'qunit';
import sinon from 'sinon';

const { readComplianceDataTypes } = complianceDataTypesApi;

module('Unit | Utility | api/list/compliance datatypes', {
  beforeEach() {
    this.xhr = sinon.useFakeXMLHttpRequest();
  },

  afterEach() {
    this.xhr.restore();
  }
});

test('readComplianceDataTypes exhibits expected behaviour', function(assert) {
  assert.ok(typeof readComplianceDataTypes({}).then === 'function', 'it returns a Promise object or thennable');
});
