import moment from 'moment';
import React, { useMemo } from 'react';
import styled from 'styled-components';

import { useMutationUrn } from '@app/entity/shared/EntityContext';
import { pathMatchesExact } from '@app/entityV2/dataset/profile/schema/utils/utils';
import NotesSection from '@app/entityV2/shared/notes/NotesSection';
import FieldDescription from '@app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/FieldDescription';
import { FieldDetails } from '@app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/FieldDetails';
import FieldTags from '@app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/FieldTags';
import FieldTerms from '@app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/FieldTerms';
import SampleValuesSection from '@app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/SampleValuesSection';
import StatsSection from '@app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/StatsSection';
import { StyledDivider } from '@app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/components';
import SidebarStructuredProperties from '@src/app/entityV2/shared/sidebarSection/SidebarStructuredProperties';

import { DatasetFieldProfile, EditableSchemaMetadata, Post, SchemaField, UsageQueryResult } from '@types';

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
        notes: Post[];
        setSelectedTabName: any;
        isShowMoreEnabled?: boolean;
        refetch?: () => void;
        refetchNotes?: () => void;
    };
}

export function AboutFieldTab({ properties }: AboutFieldTabProps) {
    const datasetUrn = useMutationUrn();
    const { refetch, refetchNotes } = properties;

    const expandedFieldIndex = useMemo(
        () => properties.schemaFields.findIndex((row) => row.fieldPath === properties.expandedDrawerFieldPath),
        [properties.expandedDrawerFieldPath, properties.schemaFields],
    );
    const expandedField =
        expandedFieldIndex !== undefined && expandedFieldIndex !== -1
            ? properties.schemaFields[expandedFieldIndex]
            : undefined;
    const editableFieldInfo = properties.editableSchemaMetadata?.editableSchemaFieldInfo?.find(
        (candidateEditableFieldInfo) =>
            pathMatchesExact(candidateEditableFieldInfo.fieldPath, expandedField?.fieldPath),
    );

    const notes = properties.notes?.sort((a, b) => moment(b.lastModified.time).diff(moment(a.lastModified.time))) || [];

    const delayedRefetchNotes = () =>
        setTimeout(() => refetchNotes?.(), 2000) && setTimeout(() => refetchNotes?.(), 5000);

    return (
        <>
            {expandedField && (
                <>
                    <FieldDetails
                        usageStats={properties.usageStats}
                        deprecation={expandedField?.schemaFieldEntity?.deprecation}
                        fieldPath={properties.expandedDrawerFieldPath}
                        refetch={() => setTimeout(() => refetch?.(), 2000)}
                        refetchNotes={delayedRefetchNotes}
                    />
                    <MetadataSections>
                        <NotesSection
                            urn={datasetUrn}
                            subResource={properties.expandedDrawerFieldPath ?? undefined}
                            notes={notes}
                            refetch={delayedRefetchNotes}
                        />
                        {!!notes?.length && <StyledDivider dashed />}
                        <FieldDescription
                            expandedField={expandedField}
                            editableFieldInfo={editableFieldInfo}
                            isShowMoreEnabled={properties.isShowMoreEnabled}
                        />
                        <FieldTags
                            expandedField={expandedField}
                            editableSchemaMetadata={properties.editableSchemaMetadata}
                        />
                        <FieldTerms
                            expandedField={expandedField}
                            editableSchemaMetadata={properties.editableSchemaMetadata}
                        />
                        <SidebarStructuredProperties
                            properties={{
                                isSchemaSidebar: true,
                                refetch,
                                fieldEntity: expandedField.schemaFieldEntity,
                            }}
                        />
                        <StatsSection
                            fieldProfile={properties.fieldProfile}
                            setSelectedTabName={properties.setSelectedTabName}
                        />
                        <SampleValuesSection fieldProfile={properties.fieldProfile} />
                    </MetadataSections>
                </>
            )}
        </>
    );
}
