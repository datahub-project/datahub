import moment from 'moment';
import React, { useMemo } from 'react';
import styled from 'styled-components';
import { SchemaFieldFieldsFragment } from '../../../../../../../../graphql/fragments.generated';
import {
    DatasetFieldProfile,
    EditableSchemaMetadata,
    Post,
    SchemaField,
    UsageQueryResult,
} from '../../../../../../../../types.generated';
import { useMutationUrn } from '../../../../../../../entity/shared/EntityContext';
import { pathMatchesNewPath } from '../../../../../../dataset/profile/schema/utils/utils';
import NotesSection from '../../../../../notes/NotesSection';
import FieldDescription from './FieldDescription';
import { FieldDetails } from './FieldDetails';
import FieldTags from './FieldTags';
import FieldTerms from './FieldTerms';
import SampleValuesSection from './SampleValuesSection';
import StatsSection from './StatsSection';
import { StyledDivider } from './components';

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
        refetch?: () => void;
    };
}

export function AboutFieldTab({ properties }: AboutFieldTabProps) {
    const datasetUrn = useMutationUrn();
    const { refetch } = properties;

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

    // Have to type cast because of line `businessAttributeDataType: type` in `businessAttribute` fragment
    const schemaFieldEntity = properties.schemaFields.find(
        (field) => field.fieldPath === properties.expandedDrawerFieldPath,
    )?.schemaFieldEntity as SchemaFieldFieldsFragment['schemaFieldEntity'];
    const notes =
        schemaFieldEntity?.notes?.relationships
            ?.map((r) => r.entity as Post)
            ?.sort((a, b) => moment(b.lastModified.time).diff(moment(a.lastModified.time))) || [];

    return (
        <>
            {expandedField && (
                <>
                    <FieldDetails
                        usageStats={properties.usageStats}
                        fieldPath={properties.expandedDrawerFieldPath}
                        refetch={() => setTimeout(() => refetch?.(), 2000)}
                    />
                    <MetadataSections>
                        <NotesSection
                            urn={datasetUrn}
                            subResource={properties.expandedDrawerFieldPath ?? undefined}
                            notes={notes}
                            refetch={() => setTimeout(() => refetch?.(), 2000)}
                        />
                        {!!notes?.length && <StyledDivider dashed />}
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
                        <SampleValuesSection fieldProfile={properties.fieldProfile} />
                    </MetadataSections>
                </>
            )}
        </>
    );
}
