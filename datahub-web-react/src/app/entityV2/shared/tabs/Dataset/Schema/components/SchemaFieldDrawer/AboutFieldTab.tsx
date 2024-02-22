import React, { useMemo } from 'react';
import styled from 'styled-components';
import {
    DatasetFieldProfile,
    EditableSchemaMetadata,
    SchemaField,
    UsageQueryResult,
} from '../../../../../../../../types.generated';
import { pathMatchesNewPath } from '../../../../../../dataset/profile/schema/utils/utils';
import FieldDescription from './FieldDescription';
import { FieldDetails } from './FieldDetails';
import FieldTags from './FieldTags';
import FieldTerms from './FieldTerms';
import SampleValuesSection from './SampleValuesSection';
import StatsSection from './StatsSection';

const MetadataSections = styled.div`
    padding: 16px 12px;
    padding-top: 0px;
`;

interface AboutFieldTabProps {
    properties: {
        schemaFields: SchemaField[];
        expandedDrawerFieldPath: string | null;
        editableSchemaMetadata?: EditableSchemaMetadata | null;
        usageStats?: UsageQueryResult | null;
        fieldProfile: DatasetFieldProfile | undefined;
        profiles: any[];
        setSelectedTabName: any;
    };
}

export function AboutFieldTab({ properties }: AboutFieldTabProps) {
    const expandedFieldIndex = useMemo(
        () => properties.schemaFields.findIndex((row) => row.fieldPath === properties.expandedDrawerFieldPath),
        [properties.expandedDrawerFieldPath, properties.schemaFields],
    );
    const expandedField =
        expandedFieldIndex !== undefined && expandedFieldIndex !== -1
            ? properties.schemaFields[expandedFieldIndex]
            : undefined;
    const editableFieldInfo = properties.editableSchemaMetadata?.editableSchemaFieldInfo.find(
        (candidateEditableFieldInfo) =>
            pathMatchesNewPath(candidateEditableFieldInfo.fieldPath, expandedField?.fieldPath),
    );

    return (
        <>
            {expandedField && (
                <>
                    <FieldDetails usageStats={properties.usageStats} fieldPath={properties.expandedDrawerFieldPath} />
                    <MetadataSections>
                        <FieldDescription expandedField={expandedField} editableFieldInfo={editableFieldInfo} />
                        <FieldTags
                            expandedField={expandedField}
                            editableSchemaMetadata={properties.editableSchemaMetadata}
                        />
                        <FieldTerms
                            expandedField={expandedField}
                            editableSchemaMetadata={properties.editableSchemaMetadata}
                        />
                        <StatsSection
                            expandedField={expandedField}
                            fieldProfile={properties.fieldProfile}
                            profiles={properties.profiles}
                            setSelectedTabName={properties.setSelectedTabName}
                        />
                        <SampleValuesSection
                            expandedField={expandedField}
                            fieldProfile={properties.fieldProfile}
                            profiles={properties.profiles}
                        />
                    </MetadataSections>
                </>
            )}
        </>
    );
}
