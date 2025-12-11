/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';

import { REDESIGN_COLORS } from '@app/entity/shared/constants';
import { DefaultViewIcon } from '@app/entity/view/shared/DefaultViewIcon';

type Props = {
    title?: React.ReactNode;
};

export const UserDefaultViewIcon = ({ title }: Props) => {
    return <DefaultViewIcon title={title} color={REDESIGN_COLORS.BLUE} />;
};
