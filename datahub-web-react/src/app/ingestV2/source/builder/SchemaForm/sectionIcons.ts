import type { Icon } from '@phosphor-icons/react';
import { ArrowsClockwise } from '@phosphor-icons/react/dist/csr/ArrowsClockwise';
import { ChartBar } from '@phosphor-icons/react/dist/csr/ChartBar';
import { Funnel } from '@phosphor-icons/react/dist/csr/Funnel';
import { Gauge } from '@phosphor-icons/react/dist/csr/Gauge';
import { Gear } from '@phosphor-icons/react/dist/csr/Gear';
import { GitBranch } from '@phosphor-icons/react/dist/csr/GitBranch';
import { Key } from '@phosphor-icons/react/dist/csr/Key';
import { Plug } from '@phosphor-icons/react/dist/csr/Plug';
import { Tag } from '@phosphor-icons/react/dist/csr/Tag';
import React from 'react';

const ICON_SIZE = 16;

const ICON_BY_NAME: Record<string, Icon> = {
    link: Plug,
    filter: Funnel,
    flow: GitBranch,
    chart: ChartBar,
    gauge: Gauge,
    tag: Tag,
    refresh: ArrowsClockwise,
    gear: Gear,
    key: Key,
};

export function sectionIcon(name?: string): React.ReactNode {
    if (!name) {
        return null;
    }
    const IconComponent = ICON_BY_NAME[name];
    if (!IconComponent) {
        return null;
    }
    return React.createElement(IconComponent, { size: ICON_SIZE });
}
