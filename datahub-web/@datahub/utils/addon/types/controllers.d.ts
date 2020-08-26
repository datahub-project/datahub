import Controller from '@ember/controller';
import { ProtocolController } from '@datahub/utils/controllers/protocol';
export interface IProtocolDefinition {
  protocol: (c: typeof ProtocolController) => typeof ProtocolController;
  queryParams: Controller['queryParams'];
}
