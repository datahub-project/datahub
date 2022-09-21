import React, { useState } from 'react';
import { Typography } from 'antd';
import styled from 'styled-components';
import Highlight from 'react-highlighter';
import translateFieldPath from './translateFieldPath';
import { ExtendedSchemaFields } from './types';
import TypeLabel from '../../../../shared/tabs/Dataset/Schema/components/TypeLabel';
import { ForeignKeyConstraint, SchemaMetadata } from '../../../../../../types.generated';
import PrimaryKeyLabel from '../../../../shared/tabs/Dataset/Schema/components/PrimaryKeyLabel';
import ForeignKeyLabel from '../../../../shared/tabs/Dataset/Schema/components/ForeignKeyLabel';

const MAX_FIELD_PATH_LENGTH = 200;

// const LighterText = styled(Typography.Text)`
//     color: rgba(0, 0, 0, 0.45);
// `;

const FieldPathContainer = styled.div`
    vertical-align: top;
    display: inline-block;
    width: 250px;
    margin-top: 16px;
    margin-bottom: 16px;
`;
const FieldPathText = styled(Typography.Text)`
    font-size: 12px;
    line-height: 22px;
    font-family: 'Roboto Mono', monospace;
    font-weight: 500;
`;

// ex: [type=MetadataAuditEvent].[type=union]oldSnapshot.[type=CorpUserSnapshot].[type=array]aspects.[type=union].[type=CorpUserInfo].[type=boolean]active
export default function useSchemaTitleRenderer(
    schemaMetadata: SchemaMetadata | undefined | null,
    setSelectedFkFieldPath: (params: { fieldPath: string; constraint?: ForeignKeyConstraint | null } | null) => void,
    filterText: string,
) {
    const [highlightedConstraint, setHighlightedConstraint] = useState<string | null>(null);

    return (fieldPath: string, record: ExtendedSchemaFields): JSX.Element => {
        const fieldPathWithoutAnnotations = translateFieldPath(fieldPath);
        const parentPathWithoutAnnotations = translateFieldPath(record.parent?.fieldPath || '');
        let pathToDisplay = fieldPathWithoutAnnotations;

        // if the parent path is a prefix of the field path, remove it for display purposes
        if (parentPathWithoutAnnotations && fieldPathWithoutAnnotations.indexOf(parentPathWithoutAnnotations) === 0) {
            // parent length + 1 because of the trailing `.` of the parent
            pathToDisplay = fieldPathWithoutAnnotations.slice(parentPathWithoutAnnotations.length + 1);
        }

        // if the field path is too long, truncate it
        if (pathToDisplay.length > MAX_FIELD_PATH_LENGTH) {
            pathToDisplay = `..${pathToDisplay.substring(pathToDisplay.length - MAX_FIELD_PATH_LENGTH)}`;
        }

        return (
            <>
                <FieldPathContainer>
                    <FieldPathText>
                        <Highlight search={filterText}>{pathToDisplay}</Highlight>
                    </FieldPathText>
                    <TypeLabel type={record.type} nativeDataType={record.nativeDataType} />
                    {(schemaMetadata?.primaryKeys?.includes(fieldPath) || record.isPartOfKey) && <PrimaryKeyLabel />}
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
                </FieldPathContainer>
            </>
        );
    };
}
