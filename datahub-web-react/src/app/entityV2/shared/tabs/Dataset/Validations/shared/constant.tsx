import { Clock } from '@phosphor-icons/react/dist/csr/Clock';
import { Database } from '@phosphor-icons/react/dist/csr/Database';
import { Dresser } from '@phosphor-icons/react/dist/csr/Dresser';
import { GitFork } from '@phosphor-icons/react/dist/csr/GitFork';
import { Hammer } from '@phosphor-icons/react/dist/csr/Hammer';
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
