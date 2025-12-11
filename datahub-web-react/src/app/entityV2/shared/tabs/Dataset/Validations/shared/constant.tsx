/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Clock, Database, Dresser, GitFork, Hammer } from '@phosphor-icons/react';
import React from 'react';

import { AssertionType } from '@src/types.generated';

export const ASSERTION_TYPE_TO_ICON_MAP: Record<AssertionType, JSX.Element> = {
    [AssertionType.Freshness]: <Clock size={20} />,
    [AssertionType.Volume]: <Database size={20} />,
    [AssertionType.Field]: <Dresser size={20} />,
    [AssertionType.DataSchema]: <GitFork size={20} />,
    [AssertionType.Custom]: <Hammer size={20} />,
    [AssertionType.Sql]: <Database size={20} />,
    [AssertionType.Dataset]: <Database size={20} />,
};
