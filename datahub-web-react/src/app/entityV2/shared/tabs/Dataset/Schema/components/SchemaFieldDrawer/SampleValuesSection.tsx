import React from 'react';
import { DatasetFieldProfile } from '../../../../../../../../types.generated';
import { SidebarSection } from '../../../../../containers/profile/sidebar/SidebarSection';
import SampleValueTag from '../../../Stats/snapshot/SampleValueTag';
import { StyledDivider } from './components';

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
                    .map((value) => (
                        <SampleValueTag value={value} />
                    ))}
            />
            <StyledDivider dashed />
        </>
    );
}
