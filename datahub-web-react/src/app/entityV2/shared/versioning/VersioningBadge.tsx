import { VersionPill } from '@app/entityV2/shared/versioning/common';
import VersionsPreview from '@app/entityV2/shared/versioning/VersionsPreview';
import { Popover } from '@components';
import { Space } from 'antd';
import React from 'react';
import { VersionProperties } from '@types';

interface Props {
    versionProperties?: VersionProperties;
    showPopover: boolean;
}

export default function VersioningBadge({ showPopover, versionProperties }: Props) {
    if (!versionProperties?.version.versionTag) {
        return null;
    }

    return (
        <Popover content={showPopover && <VersionsPreview versionSet={versionProperties.versionSet ?? undefined} />}>
            <Space>
                <VersionPill
                    label={versionProperties?.version.versionTag}
                    isLatest={versionProperties.isLatest}
                    clickable
                />
            </Space>
        </Popover>
    );
}
