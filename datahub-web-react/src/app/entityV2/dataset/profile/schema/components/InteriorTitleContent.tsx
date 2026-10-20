import { Tooltip } from '@components';
import { Typography } from 'antd';
import React from 'react';
import Highlight from 'react-highlighter';
import styled from 'styled-components';

import { useBaseEntity } from '@app/entity/shared/EntityContext';
import translateFieldPath from '@app/entityV2/dataset/profile/schema/utils/translateFieldPath';
import { ExtendedSchemaFields } from '@app/entityV2/dataset/profile/schema/utils/types';
import NullableLabel, {
    ForeignKeyLabel,
    PartitioningKeyLabel,
    PrimaryKeyLabel,
} from '@app/entityV2/shared/tabs/Dataset/Schema/components/ConstraintLabels';
import { DeprecationIcon } from '@src/app/entityV2/shared/components/styled/DeprecationIcon';

import { GetDatasetQuery } from '@graphql/dataset.generated';
import { PrimaryKeySource, SchemaMetadata, SubResourceType } from '@types';

const MAX_COMPACT_FIELD_PATH_LENGTH = 15;

const FieldTitleWrapper = styled.div<{ $isCompact: boolean }>`
    display: inline-flex;
    flex-direction: ${(props) => (props.$isCompact ? 'column' : 'row')};
    align-items: ${(props) => (props.$isCompact ? 'start' : 'center')};
    justify-content: start;
    gap: 10px;
    width: 100%;
    max-width: 100%;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
`;

const FieldPathContainer = styled.div`
    vertical-align: top;
    display: inline-block;
`;

const DeprecatedContainer = styled.div`
    color: ${(props) => props.theme.colors.textSecondary};
`;

const FieldPathText = styled(Typography.Text)<{ $isCompact: boolean }>`
    font-size: 12px;
    line-height: ${(props) => (props.$isCompact ? '14px' : '24px')};
    font-weight: 600;
    color: ${(props) => props.theme.colors.textSecondary};

    display: flex;
    align-items: center;
    gap: 6px;
`;

type InteriorTitleProps = {
    parentUrn: string;
    schemaMetadata: SchemaMetadata | undefined | null;
    filterText: string;
    fieldPath: string;
    record: ExtendedSchemaFields;
    isCompact?: boolean;
};

type DatasetKeys = Pick<
    NonNullable<NonNullable<GetDatasetQuery['dataset']>['keys']>,
    'primaryKeySource' | 'effectivePrimaryKey'
>;

// The source key comes from the displayed schema; a declared key only applies when the server
// resolved it as the effective key, i.e. when the source declares none.
export function isPrimaryKeyField(
    fieldPath: string,
    record: Pick<ExtendedSchemaFields, 'isPartOfKey'>,
    schemaMetadata: Pick<SchemaMetadata, 'primaryKeys'> | undefined | null,
    keys: DatasetKeys | undefined | null,
): boolean {
    return (
        !!schemaMetadata?.primaryKeys?.includes(fieldPath) ||
        !!record.isPartOfKey ||
        (keys?.primaryKeySource === PrimaryKeySource.Declared && keys.effectivePrimaryKey.includes(fieldPath))
    );
}

export const InteriorTitleContent = ({
    parentUrn,
    schemaMetadata,
    filterText,
    fieldPath,
    record,
    isCompact,
}: InteriorTitleProps) => {
    const isPrimaryKey = isPrimaryKeyField(
        fieldPath,
        record,
        schemaMetadata,
        useBaseEntity<GetDatasetQuery>()?.dataset?.keys,
    );
    const fieldPathWithoutAnnotations = translateFieldPath(fieldPath);
    const parentPathWithoutAnnotations = translateFieldPath(record.parent?.fieldPath || '');
    let pathToDisplay = fieldPathWithoutAnnotations;

    // if the parent path is a prefix of the field path, remove it for display purposes
    if (parentPathWithoutAnnotations && fieldPathWithoutAnnotations.indexOf(parentPathWithoutAnnotations) === 0) {
        // parent length + 1 because of the trailing `.` of the parent
        pathToDisplay = fieldPathWithoutAnnotations.slice(parentPathWithoutAnnotations.length + 1);
    }

    let compactPathToDisplay;
    if (isCompact) {
        compactPathToDisplay =
            pathToDisplay.length > MAX_COMPACT_FIELD_PATH_LENGTH
                ? `${pathToDisplay.substring(0, MAX_COMPACT_FIELD_PATH_LENGTH)}...`
                : pathToDisplay;
    }

    return (
        <FieldTitleWrapper $isCompact={!!isCompact}>
            {!!record.schemaFieldEntity?.deprecation?.deprecated && !isCompact && (
                <DeprecatedContainer>
                    <DeprecationIcon
                        urn={parentUrn}
                        subResource={fieldPath}
                        subResourceType={SubResourceType.DatasetField}
                        deprecation={record.schemaFieldEntity?.deprecation}
                        showUndeprecate={false}
                        showText={false}
                        popoverPlacement="right"
                    />
                </DeprecatedContainer>
            )}
            <FieldPathContainer>
                <FieldPathText $isCompact={!!isCompact}>
                    {isCompact ? (
                        <Tooltip title={pathToDisplay} placement="right">
                            <Highlight search={filterText}>{compactPathToDisplay}</Highlight>
                        </Tooltip>
                    ) : (
                        <Highlight search={filterText}>{pathToDisplay}</Highlight>
                    )}
                </FieldPathText>
            </FieldPathContainer>
            {!isCompact && (
                <>
                    {isPrimaryKey && <PrimaryKeyLabel />}
                    {record.isPartitioningKey && <PartitioningKeyLabel />}
                    {record.nullable && <NullableLabel />}
                    {/* {record.nullable && <NullableLabel />} */}
                    {schemaMetadata?.foreignKeys
                        ?.filter(
                            (constraint) =>
                                (constraint?.sourceFields?.filter(
                                    (sourceField) => sourceField?.fieldPath?.trim() === fieldPath.trim(),
                                ).length || 0) > 0,
                        )
                        .map((constraint) => <ForeignKeyLabel key={constraint?.name} />)}
                </>
            )}
        </FieldTitleWrapper>
    );
};
