import { Popover } from '@components';
import React from 'react';
import styled from 'styled-components';

import { PillStyleProps } from '@components/components/Pills/types';

import VersionsPreview from '@app/entityV2/shared/versioning/VersionsPreview';
import { VersionPill } from '@app/entityV2/shared/versioning/common';

import { VersionProperties } from '@types';

const PillWrapper = styled.div`
    display: inline-flex;
    align-items: center;
`;

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
            <PillWrapper>
                <VersionPill
                    {...props}
                    label={versionProperties?.version.versionTag}
                    isLatest={versionProperties.isLatest}
                    clickable={showPopover}
                    className={className}
                />
            </PillWrapper>
        </Popover>
    );
}
