import React from 'react';
import { Typography } from 'antd';
import styled from 'styled-components';
import translateFieldPath from './translateFieldPath';

const MAX_FIELD_PATH_LENGTH = 200;

const LighterText = styled(Typography.Text)`
    color: rgba(0, 0, 0, 0.45);
`;

// ex: [type=MetadataAuditEvent].[type=union]oldSnapshot.[type=CorpUserSnapshot].[type=array]aspects.[type=union].[type=CorpUserInfo].[type=boolean]active
export default function schemaTitleRenderer(fieldPath: string) {
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
        <span>
            <LighterText>{`${firstPath}${lastPath ? '.' : ''}`}</LighterText>
            {lastPath && <Typography.Text strong>{lastPath}</Typography.Text>}
        </span>
    );
}
