import { Popover } from '@components';
import { Button, Typography } from 'antd';
import React, { useState } from 'react';
import { Trans, useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { DatasetAssertionLogicModal } from '@app/entityV2/shared/tabs/Dataset/Validations/DatasetAssertionLogicModal';
import { toReadableLocalDateTimeString } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/shared/utils';
import { getFormattedParameterValue } from '@app/entityV2/shared/tabs/Dataset/Validations/assertionUtils';
import { decodeSchemaField } from '@app/lineage/utils/columnLineageUtils';

import {
    AssertionRunEvent,
    AssertionStdAggregation,
    AssertionStdOperator,
    DatasetAssertionInfo,
    DatasetAssertionScope,
    SchemaFieldRef,
} from '@types';

const ViewLogicButton = styled(Button)`
    padding: 0px;
    margin: 0px;
`;

const AssertionDetailsContainer = styled.div`
    display: flex;
    flex-direction: column;
    gap: 5px;
`;

const LastRunSection = styled.div`
    display: flex;
    gap: 5px;
`;

const TypographyContainer = styled.div`
    word-wrap: break-word;
`;

const StyledLastRunText = styled(Typography.Text)`
    font-size: 10px;
    display: flex;
    align-items: center;
`;

type Props = {
    description?: string;
    assertionInfo?: DatasetAssertionInfo;
    lastEvaluation?: AssertionRunEvent;
};

// Literal-union key parts. Keeping these as unions (not `string`) lets the composed
// `datasetDescription.${aggregation}.${operator}` key resolve to a finite key union, so the keys stay
// statically checkable once i18next typed resources (i18next.d.ts) are re-enabled.
type AggregationKey =
    | 'columnCount'
    | 'columns'
    | 'schemaNativeColumns'
    | 'rowCount'
    | 'rows'
    | 'uniqueCount'
    | 'uniqueProportion'
    | 'nullCount'
    | 'nullProportion'
    | 'min'
    | 'max'
    | 'mean'
    | 'median'
    | 'stddev'
    | 'columnValues'
    | 'dataset';

type OperatorKey =
    | 'between'
    | 'equalTo'
    | 'contains'
    | 'in'
    | 'notNull'
    | 'greaterThan'
    | 'greaterThanOrEqualTo'
    | 'lessThan'
    | 'lessThanOrEqualTo'
    | 'startsWith'
    | 'endsWith'
    | 'native'
    | 'fallback';

// Shared <Trans> components map; `bold` matches the <bold> tags in the description translation keys.
const boldComponents = { bold: <Typography.Text strong /> };

/**
 * Resolves the aggregation portion of the description key, plus the column value(s) it interpolates.
 *
 * The full description key is `datasetDescription.<aggregation>.<operator>` — one complete sentence
 * per (aggregation, operator) pair so translators control word order across the whole phrase rather
 * than across concatenated fragments.
 */
const getAggregationDescriptor = (
    scope: DatasetAssertionScope | undefined | null,
    aggregation: AssertionStdAggregation | undefined | null,
    fields: Array<SchemaFieldRef> | undefined | null,
): { key: AggregationKey; column?: string; columns?: string } => {
    switch (scope) {
        case DatasetAssertionScope.DatasetSchema:
            switch (aggregation) {
                case AssertionStdAggregation.ColumnCount:
                    return { key: 'columnCount' };
                case AssertionStdAggregation.Columns:
                    return { key: 'columns' };
                case AssertionStdAggregation.Native:
                    return {
                        key: 'schemaNativeColumns',
                        columns: JSON.stringify(fields?.map((field) => decodeSchemaField(field.path)) || []),
                    };
                default:
                    console.error(`Unsupported schema aggregation assertion ${aggregation} provided.`);
                    return { key: 'columns' };
            }
        case DatasetAssertionScope.DatasetRows:
            switch (aggregation) {
                case AssertionStdAggregation.RowCount:
                    return { key: 'rowCount' };
                case AssertionStdAggregation.Native:
                    return { key: 'rows' };
                default:
                    console.error(`Unsupported Dataset Rows Aggregation ${aggregation} provided`);
                    return { key: 'rows' };
            }
        case DatasetAssertionScope.DatasetColumn: {
            const field = fields?.length === 1 ? fields[0] : undefined;
            let column = decodeSchemaField(field?.path || '');
            if (field === undefined) {
                column = 'undefined';
                console.error(
                    `Invalid field provided for Dataset Assertion with scope Column ${JSON.stringify(field)}`,
                );
            }
            switch (aggregation) {
                case AssertionStdAggregation.UniqueCount:
                    return { key: 'uniqueCount', column };
                case AssertionStdAggregation.UniquePropotion:
                    return { key: 'uniqueProportion', column };
                case AssertionStdAggregation.NullCount:
                    return { key: 'nullCount', column };
                case AssertionStdAggregation.NullProportion:
                    return { key: 'nullProportion', column };
                case AssertionStdAggregation.Min:
                    return { key: 'min', column };
                case AssertionStdAggregation.Max:
                    return { key: 'max', column };
                case AssertionStdAggregation.Mean:
                    return { key: 'mean', column };
                case AssertionStdAggregation.Median:
                    return { key: 'median', column };
                case AssertionStdAggregation.Stddev:
                    return { key: 'stddev', column };
                // Native + any other column aggregation: treat the column as a set of values.
                default:
                    return { key: 'columnValues', column };
            }
        }
        default:
            console.error(`Unsupported Dataset Assertion scope ${scope} provided`);
            return { key: 'dataset' };
    }
};

// Resolves the operator portion of the description key.
const getOperatorKey = (op: AssertionStdOperator | undefined): OperatorKey => {
    switch (op) {
        case AssertionStdOperator.Between:
            return 'between';
        case AssertionStdOperator.EqualTo:
            return 'equalTo';
        case AssertionStdOperator.Contain:
            return 'contains';
        case AssertionStdOperator.In:
            return 'in';
        case AssertionStdOperator.NotNull:
            return 'notNull';
        case AssertionStdOperator.GreaterThan:
            return 'greaterThan';
        case AssertionStdOperator.GreaterThanOrEqualTo:
            return 'greaterThanOrEqualTo';
        case AssertionStdOperator.LessThan:
            return 'lessThan';
        case AssertionStdOperator.LessThanOrEqualTo:
            return 'lessThanOrEqualTo';
        case AssertionStdOperator.StartWith:
            return 'startsWith';
        case AssertionStdOperator.EndWith:
            return 'endsWith';
        case AssertionStdOperator.Native:
            return 'native';
        default:
            return 'fallback';
    }
};

const TOOLTIP_MAX_WIDTH = 440;

/**
 * A human-readable description of an Assertion.
 *
 * For example, Column 'X' values are in [1, 2, 3]
 */
export const DatasetAssertionDescription = ({ description, assertionInfo, lastEvaluation }: Props) => {
    const { t } = useTranslation('entity.profile.validations');
    const { t: tc } = useTranslation('common.labels');
    const { scope, aggregation, fields, operator, parameters, nativeType, nativeParameters, logic } =
        assertionInfo ?? {};
    const [isLogicVisible, setIsLogicVisible] = useState(false);

    // Build the full-sentence description key from the aggregation + operator dimensions.
    const agg = getAggregationDescriptor(scope, aggregation, fields);
    const operatorKey = getOperatorKey(operator || undefined);
    const descriptionFragment = (
        <>
            {description || (
                <Typography.Text>
                    <Trans
                        t={t}
                        i18nKey={`datasetDescription.${agg.key}.${operatorKey}`}
                        values={{
                            column: agg.column ?? '',
                            columns: agg.columns ?? '',
                            value: getFormattedParameterValue(parameters?.value),
                            minValue: getFormattedParameterValue(parameters?.minValue),
                            maxValue: getFormattedParameterValue(parameters?.maxValue),
                            nativeType: nativeType ?? '',
                            operator: operator ?? '',
                        }}
                        components={boldComponents}
                    />
                </Typography.Text>
            )}
        </>
    );

    return (
        <Popover
            overlayStyle={{ maxWidth: TOOLTIP_MAX_WIDTH }}
            title={<Typography.Text strong>{tc('details')}</Typography.Text>}
            content={
                <>
                    <AssertionDetailsContainer>
                        {lastEvaluation?.lastObservedMillis && (
                            <LastRunSection>
                                <Typography.Text strong>{t('datasetDescription.popover.reported')}</Typography.Text>
                                <StyledLastRunText>
                                    {lastEvaluation?.lastObservedMillis &&
                                        toReadableLocalDateTimeString(lastEvaluation.lastObservedMillis)}
                                </StyledLastRunText>
                            </LastRunSection>
                        )}
                        <TypographyContainer>
                            <div>
                                <Typography.Text strong>{t('datasetDescription.popover.type')}</Typography.Text>:{' '}
                                {nativeType || tc('na')}
                            </div>
                            {nativeParameters?.map((parameter) => (
                                <div key={parameter.key}>
                                    <Typography.Text strong>{parameter.key}</Typography.Text>: {parameter.value}
                                </div>
                            ))}
                        </TypographyContainer>
                    </AssertionDetailsContainer>
                </>
            }
        >
            <div>{descriptionFragment}</div>
            {logic && (
                <div>
                    <ViewLogicButton onClick={() => setIsLogicVisible(true)} type="link">
                        {t('datasetDescription.popover.viewLogic')}
                    </ViewLogicButton>
                </div>
            )}
            <DatasetAssertionLogicModal
                logic={logic || tc('na')}
                visible={isLogicVisible}
                onClose={() => setIsLogicVisible(false)}
            />
        </Popover>
    );
};
