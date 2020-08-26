import { module, test } from 'qunit';
import { WithControllerProtocol, ProtocolController } from '@datahub/utils/controllers/protocol';
import { IProtocolDefinition } from '@datahub/utils/types/controllers';

module('Unit | Utility | controllers/protocol', function() {
  test('it joins queryParams successfully', function(assert): void {
    const myProtocol1: IProtocolDefinition = {
      protocol: (controller: typeof ProtocolController): typeof ProtocolController => class extends controller {},
      queryParams: ['one']
    };
    const myProtocol2: IProtocolDefinition = {
      protocol: (controller: typeof ProtocolController): typeof ProtocolController => class extends controller {},
      queryParams: ['two']
    };
    class MyController extends WithControllerProtocol(myProtocol1, myProtocol2) {}

    const controller = MyController.create();
    assert.deepEqual(controller.queryParams, ['one', 'two'], 'query params matches');
  });
});
