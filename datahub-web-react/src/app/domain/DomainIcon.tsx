/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import Icon from '@ant-design/icons/lib/components/Icon';
import React from 'react';

import DomainsIcon from '@images/domain.svg?react';

type Props = {
    style?: React.CSSProperties;
};

export default function DomainIcon({ style }: Props) {
    return <Icon component={DomainsIcon} style={style} />;
}
