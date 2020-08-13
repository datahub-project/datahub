import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import { INotification } from '@datahub/utils/types/notifications/service';
import { NotificationEvent, NotificationType } from '@datahub/utils/constants/notifications';
import { noop } from 'lodash';

const toastBaseClass = '.notifications__toast';
const makeToast: (props?: { content?: string }) => INotification = (props = { content: 'Success!' }) => ({
  props: {
    type: NotificationEvent.success,
    content: props.content
  },
  type: NotificationType.Toast,
  notificationResolution: {
    createPromiseToHandleThisNotification: () => Promise.resolve()
  }
});

module('Integration | Component | notifications-toast', function(hooks): void {
  setupRenderingTest(hooks);

  test('toast rendering', async function(assert): Promise<void> {
    this.set('onDismiss', noop);
    await render(hbs`<NotificationsToast @onDismiss={{this.onDismiss}}/>`);

    assert.dom(toastBaseClass).exists();
    assert.dom(`${toastBaseClass}__dismiss`).hasText('×');

    await render(hbs`
    <NotificationsToast @onDismiss={{this.onDismiss}} as |Toast|>
      {{Toast.content}}
    </NotificationsToast>`);

    assert.dom(`${toastBaseClass}__content__msg`).exists();
  });

  test('toast property rendering', async function(assert): Promise<void> {
    const fakeToast = makeToast();
    const { content = '' } = fakeToast.props;
    this.setProperties({ onDismiss: noop, toast: fakeToast });
    await render(hbs`<NotificationsToast @onDismiss={{this.onDismiss}} @toast={{this.toast}}/>`);

    assert.dom(`${toastBaseClass}__content--success`).exists();
    assert.dom(`${toastBaseClass}__content__msg`).hasText(content);
  });

  test('toast content truncation and content detail', async function(assert): Promise<void> {
    const fakeToast = makeToast({
      content:
        'A long string of text for user notification that contains more than the required number of characters to be displayed in a toast'
    });
    this.setProperties({ onDismiss: noop, toast: fakeToast });
    await render(
      hbs`<NotificationsToast @onDismiss={{this.onDismiss}} @toast={{this.toast}} @onShowDetail={{this.onDismiss}} />`
    );

    assert
      .dom(`${toastBaseClass}__content__msg`)
      .hasText('A long string of text for user notification that...number of characters to be displayed in a toast');

    assert.dom(`${toastBaseClass}__content-detail`).hasText('See Detail');
  });
});
