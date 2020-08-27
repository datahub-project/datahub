// import { transformChangeLogDetailResponse } from '@datahub/shared/utils/change-management';
import { module, test } from 'qunit';
import changeLogScenario from '@datahub/shared/mirage-addon/scenarios/change-log';
import setupMirage from 'ember-cli-mirage/test-support/setup-mirage';
import { MirageTestContext } from '@datahub/utils/types/vendor/ember-cli-mirage/mirage-tests';
import { getSerializedMirageModel } from '@datahub/utils/test-helpers/serialize-mirage-model';
import { setupTest } from 'ember-qunit';
import { transformChangeLogDetailResponse, constructChangeLogContent } from '@datahub/shared/utils/change-management';
import { IChangeLogProperties } from '@datahub/shared/modules/change-log';
import { IAddChangeLogModalProps } from '@datahub/shared/types/change-management/change-log';

module('Unit | Utility | Change-Management', function(hooks): void {
  setupTest(hooks);
  setupMirage(hooks);

  test('transformChangeLogDetailResponse method works as expected', function(this: MirageTestContext, assert): void {
    changeLogScenario(this.server);
    const testChangeLog: Com.Linkedin.DataConstructChangeManagement.DataConstructChangeManagement = getSerializedMirageModel(
      'changeLogs',
      this.server
    )[0];
    const transformedChangeLog: IChangeLogProperties = transformChangeLogDetailResponse(testChangeLog);
    assert.equal(transformedChangeLog.id, testChangeLog.id);
    assert.equal(transformedChangeLog.createdBy, testChangeLog.lastModified.actor);
    assert.equal(transformedChangeLog.subject, testChangeLog.message.subject);
    assert.equal(transformedChangeLog.content, testChangeLog.message.messageText);
    assert.equal(transformedChangeLog.sendEmail, testChangeLog?.notification?.notificationTypes.email);
  });

  test('constructChangeLogContent method works as expected', function(this: MirageTestContext, assert): void {
    const testNewChangeLogInfo: IAddChangeLogModalProps = {
      subject: 'AMD processors currently have no driver support',
      content: '',
      sendEmail: true,
      recipients: [{ userUrn: 'pikachu' }, { userUrn: 'witcher' }]
    };

    const changeLogContent: Com.Linkedin.DataConstructChangeManagement.DataConstructChangeManagementContent = constructChangeLogContent(
      testNewChangeLogInfo,
      'ranbalag',
      'li:AMD:Ryzen3600',
      'dataset'
    );

    assert.equal(changeLogContent.lastModified.actor, 'urn:li:corpuser:ranbalag');
    assert.equal(changeLogContent.message.subject, testNewChangeLogInfo.subject);
    assert.equal(changeLogContent.message.messageText, testNewChangeLogInfo.content);
    assert.equal(changeLogContent.notification?.recipients, testNewChangeLogInfo.recipients);
    assert.equal(changeLogContent.notification?.notificationTypes.email, true);
  });
});
