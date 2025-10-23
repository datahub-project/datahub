import { ArgsProps } from 'antd/lib/notification';

import { DATAHUB_CLASS_NAME } from '@components/components/Notification/constants';

export const defaults: Partial<ArgsProps> = {
    placement: 'topRight',
    className: DATAHUB_CLASS_NAME,
};
