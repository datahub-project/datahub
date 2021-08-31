import React from 'react';
import { Typography } from 'antd';
import styled from 'styled-components';
import translateFieldPath from './translateFieldPath';
import { ExtendedSchemaFields } from './types';
import TypeLabel from '../../../../shared/tabs/Dataset/Schema/components/TypeLabel';

const MAX_FIELD_PATH_LENGTH = 200;

// const LighterText = styled(Typography.Text)`
//     color: rgba(0, 0, 0, 0.45);
// `;

const FieldPathContainer = styled.div`
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
export default function schemaTitleRenderer(fieldPath: string, record: ExtendedSchemaFields) {
    const fieldPathWithoutAnnotations = translateFieldPath(fieldPath);

    const isOverflow = fieldPathWithoutAnnotations.length > MAX_FIELD_PATH_LENGTH;

    let [firstPath, lastPath] = fieldPathWithoutAnnotations.split(/\.(?=[^.]+$)/);

    if (isOverflow) {
        if (lastPath.length >= MAX_FIELD_PATH_LENGTH) {
            lastPath = `..${lastPath.substring(lastPath.length - MAX_FIELD_PATH_LENGTH)}`;
            firstPath = '';
        } else {
            firstPath = firstPath.substring(fieldPath.length - MAX_FIELD_PATH_LENGTH);
            if (firstPath.includes('.')) {
                firstPath = `..${firstPath.substring(firstPath.indexOf('.'))}`;
            } else {
                firstPath = '..';
            }
        }
    }

    return (
        <FieldPathContainer>
            <FieldPathText>{lastPath || firstPath}</FieldPathText>
            <TypeLabel type={record.type} nativeDataType={record.nativeDataType} />
        </FieldPathContainer>
    );
}
