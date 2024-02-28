import React from 'react';
import { DatasetFieldProfile, SchemaField } from '../../../../../../../../types.generated';
import { SidebarSection } from '../../../../../containers/profile/sidebar/SidebarSection';
import SampleValueTag from '../../../Stats/snapshot/SampleValueTag';
import { StyledDivider } from './components';

interface Props {
    expandedField: SchemaField;
    fieldProfile: DatasetFieldProfile | undefined;
    profiles: any[];
}

export default function SampleValuesSection({ expandedField, fieldProfile, profiles }: Props) {
    const historicFieldProfiles = profiles.filter((profile) =>
        profile.fieldProfiles?.some((fieldProf) => fieldProf.fieldPath === expandedField.fieldPath),
    );

    // If current field profile doesn't exist or historic profiles don't have multiple profiles of the current field
    if (!fieldProfile) return null;

    return (
        <>
            <SidebarSection
                title="Sample Values"
                content={fieldProfile.sampleValues
                    ?.filter((value) => value !== undefined)
                    .slice(0, 3)
                    .map((value, idx) => (
                        <SampleValueTag value={value} key={`sample-value-${idx}`} />
                    ))}
            />
            <StyledDivider dashed />
        </>
    );
}
