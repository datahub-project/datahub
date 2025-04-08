import { ScanOutlined } from '@ant-design/icons';
import { Button, Tooltip, Typography } from 'antd';
import React from 'react';
import { useHistory, useLocation } from 'react-router-dom';
import styled from 'styled-components';

import { pathMatchesNewPath } from '@app/entity/dataset/profile/schema/utils/utils';
import { navigateToVersionedDatasetUrl } from '@app/entity/shared/tabs/Dataset/Schema/utils/navigateToVersionedDatasetUrl';
import { toRelativeTimeString } from '@app/shared/time/timeUtils';

import { SchemaField, SchemaFieldBlame } from '@types';

const HeadingDiv = styled.div`
    vertical-align: top;
    display: flex;
    flex-direction: column;
    margin-top: 16px;
`;

const SubheadingDiv = styled.div`
    display: flex;
    flex-wrap: wrap;
    justify-content: space-between;
`;

const SchemaBlameText = styled(Typography.Text)`
    font-size: 14x;
    line-height: 22px;
    font-family: 'Roboto Mono', monospace;
    font-weight: 500;
`;

const SchemaBlameTimestampText = styled(Typography.Text)`
    font-size: 8px;
    line-height: 22px;
    font-family: 'Roboto Mono', monospace;
    font-weight: 500;
`;

const SchemaBlameBlameButton = styled(Button)`
    display: inline-block;
    width: 30px;
`;

export default function useSchemaBlameRenderer(schemaBlameList?: Array<SchemaFieldBlame> | null) {
    const history = useHistory();
    const location = useLocation();
    const schemaBlameRenderer = (record: SchemaField) => {
        const relevantSchemaFieldBlame = schemaBlameList?.find((candidateSchemaBlame) =>
            pathMatchesNewPath(candidateSchemaBlame.fieldPath, String(record)),
        );

        if (!relevantSchemaFieldBlame || !relevantSchemaFieldBlame.schemaFieldChange) {
            return null;
        }

        return (
            <>
                <HeadingDiv>
                    <SchemaBlameText data-testid={`${relevantSchemaFieldBlame.fieldPath}-schema-blame-description`}>
                        {relevantSchemaFieldBlame?.schemaFieldChange?.lastSchemaFieldChange}
                    </SchemaBlameText>
                    <SubheadingDiv>
                        {relevantSchemaFieldBlame?.schemaFieldChange?.timestampMillis ? (
                            <SchemaBlameTimestampText>
                                {toRelativeTimeString(relevantSchemaFieldBlame?.schemaFieldChange?.timestampMillis)}
                            </SchemaBlameTimestampText>
                        ) : (
                            'unknown'
                        )}
                        <span>
                            <Tooltip title="View blame prior to this version">
                                <SchemaBlameBlameButton
                                    data-testid={`${relevantSchemaFieldBlame.fieldPath}-view-prior-blame-button`}
                                    onClick={() => {
                                        navigateToVersionedDatasetUrl({
                                            location,
                                            history,
                                            datasetVersion:
                                                relevantSchemaFieldBlame.schemaFieldChange.lastSemanticVersion,
                                        });
                                    }}
                                    size="small"
                                    type="text"
                                >
                                    <ScanOutlined style={{}} />
                                </SchemaBlameBlameButton>
                            </Tooltip>
                        </span>
                    </SubheadingDiv>
                </HeadingDiv>
            </>
        );
    };
    return schemaBlameRenderer;
}
