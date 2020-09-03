import { Factory } from 'ember-cli-mirage';
import Faker from 'faker';

type ChangeLogDetailResponse = Com.Linkedin.DataConstructChangeManagement.DataConstructChangeManagement;

const recipient = (): Com.Linkedin.DataConstructChangeManagement.NotificationRecipient => {
  return {
    userUrn: Faker.name.firstName().slice(0, 8)
  };
};

export default Factory.extend<Com.Linkedin.DataConstructChangeManagement.DataConstructChangeManagement>({
  owningEntity(): Com.Linkedin.DataConstructChangeManagement.OwningEntity {
    return {
      [Faker.random.arrayElement(['dataset', 'metric', 'feature'])]: 'testEntityUrn'
    } as Com.Linkedin.DataConstructChangeManagement.OwningEntity;
  },
  category(): Com.Linkedin.DataConstructChangeManagement.Category {
    return Faker.random.arrayElement([
      'BUSINESS_LOGIC',
      'DEPRECATION',
      'LINEAGE',
      'REMOVAL',
      'SCHEDULING',
      'SCHEMA_UPDATE',
      'OTHER'
    ]);
  },
  message(): Com.Linkedin.DataConstructChangeManagement.Message {
    return {
      subject: Faker.lorem.text(15),
      messageText: Faker.lorem.paragraph(5)
    };
  },
  notification(): Com.Linkedin.DataConstructChangeManagement.Notification {
    return {
      recipients: new Array(Faker.random.number({ min: 1, max: 5 })).fill(recipient),
      notificationTypes: {
        jira: Faker.random.boolean(),
        // setting this to true since atleast one of the types should be `true` if there is atleast one recipient present!
        email: true,
        banner: Faker.random.boolean()
      }
    };
  },
  lastModified(): Com.Linkedin.Common.AuditStamp {
    return {
      time: Faker.random.number({ min: 1000000000000, max: 2000000000000 }),
      actor: Faker.name.firstName()
    };
  },
  id: Faker.random.number()
});

declare module 'ember-cli-mirage/types/registries/schema' {
  interface IMirageSchemaRegistry {
    changeLogs: ChangeLogDetailResponse;
    changeLog?: ChangeLogDetailResponse;
  }
}

declare module 'ember-cli-mirage/types/registries/model' {
  interface IMirageModelRegistry {
    changeLogs: ChangeLogDetailResponse;
    changeLog?: ChangeLogDetailResponse;
  }
}
