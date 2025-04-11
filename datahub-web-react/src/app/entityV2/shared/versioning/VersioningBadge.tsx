import { VersionPill } from '@app/entityV2/shared/versioning/common';
import VersionsPreview from '@app/entityV2/shared/versioning/VersionsPreview';
import { Popover } from '@components';
import { PillStyleProps } from '@components/components/Pills/types';
import { Space } from 'antd';
import React from 'react';
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
