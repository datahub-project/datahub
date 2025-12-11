/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Popover } from '@components';
import { Space } from 'antd';
import React from 'react';

import { PillStyleProps } from '@components/components/Pills/types';

import VersionsPreview from '@app/entityV2/shared/versioning/VersionsPreview';
import { VersionPill } from '@app/entityV2/shared/versioning/common';

import { VersionProperties } from '@types';

interface Props {
    versionProperties?: VersionProperties;
    showPopover: boolean;
    className?: string;
}

export default function VersioningBadge({
    showPopover,
    versionProperties,
    className,
    ...props
}: Props & Partial<PillStyleProps>) {
    if (!versionProperties?.version.versionTag) {
        return null;
    }

    return (
        <Popover content={showPopover && <VersionsPreview versionSet={versionProperties.versionSet ?? undefined} />}>
            <Space>
                <VersionPill
                    {...props}
                    label={versionProperties?.version.versionTag}
                    isLatest={versionProperties.isLatest}
                    clickable={showPopover}
                    className={className}
                />
            </Space>
        </Popover>
    );
}
