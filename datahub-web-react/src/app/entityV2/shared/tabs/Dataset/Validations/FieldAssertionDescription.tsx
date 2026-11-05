import { Pill, Text } from '@components';
import { Typography } from 'antd';
import { Maybe } from 'graphql/jsutils/Maybe';
import i18next from 'i18next';
import React from 'react';
import { Trans, useTranslation } from 'react-i18next';
import styled from 'styled-components';

import {
    getFieldDescription,
    getFieldDescriptionDescriptor,
} from '@app/entityV2/shared/tabs/Dataset/Validations/fieldDescriptionUtils';

import { FieldAssertionInfo } from '@types';

type Props = {
    assertionInfo: FieldAssertionInfo;
    showColumnTag?: boolean;
    // use below description which is present in assertion info object to decide whether to show description or generate dynamic one
    assertionDescription?: Maybe<string>;
    ellipsis?: boolean;
};

const BoldText = styled(Text).attrs({ type: 'span', weight: 'bold' })`
    color: ${(props) => props.theme.colors.text};
    font-size: inherit;
    line-height: inherit;
    word-break: break-word;
`;

const StyledDescrptionContainer = styled.div`
    display: flex;
    flex-direction: column;
    gap: 4px;
`;
/**
 * A human-readable description of a Field Assertion.
 * @param showColumnTag decide whether to show column tag or not
 * e.g.
 * consider user selected field -> profileId, operator -> greater than, parameters-> 5
 * if @param showColumnTag is true then description will be -> column values are greater than 5
 * if @param showColumnTag is false then description will be -> profileId is greater than 5
 */
const renderFallbackDescription = (field: string | null | undefined) => (
    <Trans
        i18nKey="entity.profile.validations:fieldDescription.customCheckOn"
        values={{ field: field ?? i18next.t('entity.profile.validations:fieldDescription.fieldFallback') }}
        components={{ bold: <BoldText /> }}
    />
);

export const FieldAssertionDescription = ({ assertionInfo, showColumnTag, assertionDescription, ellipsis }: Props) => {
    const { t } = useTranslation('entity.profile.validations');
    const field = getFieldDescription(assertionInfo);
    let descriptionContent: React.ReactNode = assertionDescription;

    if (!assertionDescription) {
        try {
            const descriptor = getFieldDescriptionDescriptor(assertionInfo, {
                showColumnTag,
            });
            descriptionContent = (
                <Trans
                    t={t}
                    i18nKey={`fieldDescription.${descriptor.shape}.${descriptor.operatorKey}`}
                    values={{
                        field: descriptor.field,
                        transform: descriptor.transformLabelKey ? t(descriptor.transformLabelKey) : '',
                        metric: descriptor.metricLabelKey ? t(descriptor.metricLabelKey) : '',
                        value: descriptor.tokens.value,
                        minValue: descriptor.tokens.minValue,
                        maxValue: descriptor.tokens.maxValue,
                    }}
                    components={{
                        bold: <BoldText />,
                    }}
                />
            );
        } catch (e) {
            // Helpers throw on unsupported enum values (e.g. operator = _NATIVE_ from external
            // integrations). Without this catch, the whole Assertions tab error-boundaries out.
            console.warn('Failed to render field assertion description', e);
            descriptionContent = renderFallbackDescription(field);
        }
    }

    return (
        <StyledDescrptionContainer>
            <Typography.Text ellipsis={ellipsis ? { tooltip: true } : undefined}>{descriptionContent}</Typography.Text>
            {showColumnTag && !!field && (
                <Pill label={field} variant="filled" color="gray" size="sm" clickable={false} />
            )}
        </StyledDescrptionContainer>
    );
};
