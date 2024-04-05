import React from 'react';
import styled from 'styled-components';
import Highlight from 'react-highlighter';
import { Typography } from 'antd';
import { ForeignKeyConstraint, SchemaMetadata } from '../../../../../../types.generated';
import { REDESIGN_COLORS } from '../../../../shared/constants';
import translateFieldPath from '../utils/translateFieldPath';
import { ExtendedSchemaFields } from '../utils/types';
import PartitioningKeyLabel from '../../../../shared/tabs/Dataset/Schema/components/PartitioningKeyLabel';
import PrimaryKeyLabel from '../../../../shared/tabs/Dataset/Schema/components/PrimaryKeyLabel';
import ForeignKeyLabel from '../../../../shared/tabs/Dataset/Schema/components/ForeignKeyLabel';

const FieldTitleWrapper = styled.div`
    display: inline-flex;
    align-items: center;
    justify-content: start;
    gap: 10px;
    width: 100%;
    max-width: 100%;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    -webkit-mask: linear-gradient(-270deg, #736ba4 60%, rgba(115, 107, 164, 0) 100%);
`;

// const IconContainer = styled.div`
//     display: inline-flex;
// `;
const FieldPathContainer = styled.div`
    vertical-align: top;
    display: inline-block;
`;
const FieldPathText = styled(Typography.Text)<{ $isCompact: boolean }>`
    font-size: 12px;
    line-height: ${(props) => (props.$isCompact ? '14px' : '24px')};
    font-weight: 600;
    color: ${REDESIGN_COLORS.DARK_GREY};
`;

type InteriorTitleProps = {
    schemaMetadata: SchemaMetadata | undefined | null;
    setSelectedFkFieldPath: (params: { fieldPath: string; constraint?: ForeignKeyConstraint | null } | null) => void;
    highlightedConstraint: string | null;
    setHighlightedConstraint: (constraint: string | null) => void;
    filterText: string;
    fieldPath: string;
    record: ExtendedSchemaFields;
    isCompact?: boolean;
};

export const InteriorTitleContent = ({
    schemaMetadata,
    setSelectedFkFieldPath,
    highlightedConstraint,
    setHighlightedConstraint,
    filterText,
    fieldPath,
    record,
    isCompact,
}: InteriorTitleProps) => {
    const fieldPathWithoutAnnotations = translateFieldPath(fieldPath);
    const parentPathWithoutAnnotations = translateFieldPath(record.parent?.fieldPath || '');
    let pathToDisplay = fieldPathWithoutAnnotations;

    // if the parent path is a prefix of the field path, remove it for display purposes
    if (parentPathWithoutAnnotations && fieldPathWithoutAnnotations.indexOf(parentPathWithoutAnnotations) === 0) {
        // parent length + 1 because of the trailing `.` of the parent
        pathToDisplay = fieldPathWithoutAnnotations.slice(parentPathWithoutAnnotations.length + 1);
    }

    // if the field path is too long, truncate it
    // if (pathToDisplay.length > MAX_FIELD_PATH_LENGTH) {
    //     pathToDisplay = `..${pathToDisplay.substring(pathToDisplay.length - MAX_FIELD_PATH_LENGTH)}`;
    // }

    return (
        <FieldTitleWrapper>
            <FieldPathContainer>
                <FieldPathText $isCompact={!!isCompact}>
                    <Highlight search={filterText}>{pathToDisplay}</Highlight>
                </FieldPathText>
            </FieldPathContainer>
            {(schemaMetadata?.primaryKeys?.includes(fieldPath) || record.isPartOfKey) && <PrimaryKeyLabel />}
            {record.isPartitioningKey && <PartitioningKeyLabel />}
            {/* {record.nullable && <NullableLabel />} */}
            {schemaMetadata?.foreignKeys
                ?.filter(
                    (constraint) =>
                        (constraint?.sourceFields?.filter(
                            (sourceField) => sourceField?.fieldPath.trim() === fieldPath.trim(),
                        ).length || 0) > 0,
                )
                .map((constraint) => (
                    <ForeignKeyLabel
                        key={constraint?.name}
                        fieldPath={fieldPath}
                        constraint={constraint}
                        highlight={constraint?.name === highlightedConstraint}
                        setHighlightedConstraint={setHighlightedConstraint}
                        onClick={setSelectedFkFieldPath}
                    />
                ))}
            {/* <IconContainer>
                <AnnouncementIcon />
            </IconContainer> */}
        </FieldTitleWrapper>
    );
};
