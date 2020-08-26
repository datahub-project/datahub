import { Factory, faker } from 'ember-cli-mirage';
import { ArrayElement } from '@datahub/utils/types/array';

type userConsumer = ArrayElement<Com.Linkedin.Common.EntityTopUsage['mostFrequentUsers']>;
type groupConsumer = ArrayElement<Com.Linkedin.Common.EntityTopUsage['mostFrequentGroups']>;
type topConsumer = userConsumer | groupConsumer;

const genCorpUserConsumer = (): userConsumer => {
  return {
    accessCount: faker.random.number(100),
    identity: {
      corpUser: faker.name.firstName()
    }
  };
};

const genGridGroupConsumer = (): groupConsumer => {
  return {
    accessCount: faker.random.number(100),
    identity: {
      gridGroup: faker.name.firstName()
    }
  };
};

const sortAccessCountDescending = (a: topConsumer, b: topConsumer): number => b.accessCount - a.accessCount;

export default Factory.extend<Com.Linkedin.Common.EntityTopUsage>({
  mostFrequentGroups(): Array<groupConsumer> {
    return new Array(faker.random.number({ min: 1, max: 5 }))
      .fill(genGridGroupConsumer())
      .sort(sortAccessCountDescending);
  },

  mostFrequentUsers(): Array<userConsumer> {
    return new Array(faker.random.number({ min: 1, max: 5 }))
      .fill(genCorpUserConsumer())
      .sort(sortAccessCountDescending);
  }
});

declare module 'ember-cli-mirage/types/registries/schema' {
  interface IMirageSchemaRegistry {
    topConsumers: Com.Linkedin.Common.EntityTopUsage;
    topConsumer?: Com.Linkedin.Common.EntityTopUsage;
  }
}

declare module 'ember-cli-mirage/types/registries/model' {
  interface IMirageModelRegistry {
    topConsumers: Com.Linkedin.Common.EntityTopUsage;
    topConsumer?: Com.Linkedin.Common.EntityTopUsage;
  }
}
