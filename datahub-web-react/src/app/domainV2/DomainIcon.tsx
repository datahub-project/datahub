/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Globe } from '@phosphor-icons/react';
import React from 'react';

import { TYPE_ICON_CLASS_NAME } from '@app/entityV2/shared/components/subtypes';

type Props = {
    style?: React.CSSProperties;
};

export default function DomainIcon({ style }: Props) {
    return <Globe className={TYPE_ICON_CLASS_NAME} style={style} />;
}
