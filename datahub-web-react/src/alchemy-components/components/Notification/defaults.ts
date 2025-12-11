/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { ArgsProps } from 'antd/lib/notification';

import { DATAHUB_CLASS_NAME } from '@components/components/Notification/constants';

export const defaults: Partial<ArgsProps> = {
    placement: 'topRight',
    className: DATAHUB_CLASS_NAME,
};
