import { FormattedNotificationSetting } from '@app/settingsV2/notifications/types';
import { isParamEnabled, isParamPresent, isSinkNotificationTypeEnabled } from '@app/settingsV2/notifications/utils';

import { NotificationSettingValue } from '@types';

describe('isParamPresent', () => {
    it('should return true if the key exists in the map', () => {
        const params = new Map([['key1', 'value1']]);
        expect(isParamPresent(params, 'key1')).toBe(true);
    });

    it('should return false if the key does not exist in the map', () => {
        const params = new Map();
        expect(isParamPresent(params, 'key1')).toBe(false);
    });
});

describe('isParamEnabled', () => {
    it('should return true if the key has value "true"', () => {
        const params = new Map([['flag', 'true']]);
        expect(isParamEnabled(params, 'flag')).toBe(true);
    });

    it('should return false if the key has a value other than "true"', () => {
        const params = new Map([['flag', 'false']]);
        expect(isParamEnabled(params, 'flag')).toBe(false);

        const params2 = new Map([['flag', 'something-else']]);
        expect(isParamEnabled(params2, 'flag')).toBe(false);
    });

    it('should return false if the key is not present', () => {
        const params = new Map();
        expect(isParamEnabled(params, 'flag')).toBe(false);
    });
});

describe('isSinkNotificationTypeEnabled', () => {
    const mockSetting = (
        type: string,
        value: NotificationSettingValue,
        params: Map<string, string>,
    ): FormattedNotificationSetting => ({
        type: type as any, // safe because we're mocking
        value,
        params,
    });

    it('should return true when isEnabledByDefault is true and no setting exists', () => {
        const result = isSinkNotificationTypeEnabled('email', null, true);
        expect(result).toBe(true);
    });

    it('should return true when isEnabledByDefault is true and param is not present', () => {
        const setting = mockSetting('email', NotificationSettingValue.Enabled, new Map());
        const result = isSinkNotificationTypeEnabled('email', setting, true);
        expect(result).toBe(true);
    });

    it('should return false when isEnabledByDefault is false and no setting exists', () => {
        const result = isSinkNotificationTypeEnabled('email', null, false);
        expect(result).toBe(false);
    });

    it('should return true if setting is enabled and corresponding param is true', () => {
        const params = new Map([['email.enabled', 'true']]);
        const setting = mockSetting('email', NotificationSettingValue.Enabled, params);
        const result = isSinkNotificationTypeEnabled('email', setting, false);
        expect(result).toBe(true);
    });

    it('should return false if setting is enabled but param is false', () => {
        const params = new Map([['email.enabled', 'false']]);
        const setting = mockSetting('email', NotificationSettingValue.Enabled, params);
        const result = isSinkNotificationTypeEnabled('email', setting, false);
        expect(result).toBe(false);
    });

    it('should return false if setting is disabled even if param is true', () => {
        const params = new Map([['email.enabled', 'true']]);
        const setting = mockSetting('email', NotificationSettingValue.Disabled, params);
        const result = isSinkNotificationTypeEnabled('email', setting, false);
        expect(result).toBe(false);
    });

    it('should use correct param key based on sinkId', () => {
        const params = new Map([['slack.enabled', 'true']]);
        const setting = mockSetting('slack', NotificationSettingValue.Enabled, params);
        const result = isSinkNotificationTypeEnabled('slack', setting, false);
        expect(result).toBe(true);
    });
});
