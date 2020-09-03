import { Factory } from 'ember-cli-mirage';
import { testUrn, testTarget } from '@datahub/shared/mirage-addon/test-helpers/entity-configs';

// The interface for the feature config entry field in mirage
// TODO META-11235: Allow for entity feature configs container to batch targets
interface IEntityFeatureConfigEntry {
  urn: string;
  target: string;
}
// For some reason this file gets deleted if named `*-config.ts`
export default Factory.extend<IEntityFeatureConfigEntry>({
  urn: testUrn,
  target: testTarget
});

declare module 'ember-cli-mirage/types/registries/schema' {
  interface IMirageSchemaRegistry {
    entityFeatureConfs: IEntityFeatureConfigEntry;
    entityFeatureConf?: IEntityFeatureConfigEntry;
  }
}

declare module 'ember-cli-mirage/types/registries/model' {
  interface IMirageModelRegistry {
    entityFeatureConfs: IEntityFeatureConfigEntry;
    entityFeatureConf?: IEntityFeatureConfigEntry;
  }
}
