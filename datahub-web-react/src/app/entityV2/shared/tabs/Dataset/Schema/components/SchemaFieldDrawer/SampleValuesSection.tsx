import React from 'react';

import { SidebarSection } from '@app/entityV2/shared/containers/profile/sidebar/SidebarSection';
import { StyledDivider } from '@app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/components';
import SampleValueTag from '@app/entityV2/shared/tabs/Dataset/Stats/snapshot/SampleValueTag';

import { DatasetFieldProfile } from '@types';

interface Props {
    fieldProfile: DatasetFieldProfile | undefined;
}

export default function SampleValuesSection({ fieldProfile }: Props) {
    // If current field profile doesn't exist or historic profiles don't have multiple profiles of the current field
    if (!fieldProfile) return null;

    return (
        <>
            <SidebarSection
                title="Sample Values"
                content={fieldProfile.sampleValues
                    ?.filter((value) => value !== undefined)
                    .slice(0, 3)
                    .map((value) => <SampleValueTag value={value} />)}
            />
            <StyledDivider dashed />
        </>
    );
}
