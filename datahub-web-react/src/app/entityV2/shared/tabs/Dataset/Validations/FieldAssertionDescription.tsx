import { Typography } from 'antd';
import { Maybe } from 'graphql/jsutils/Maybe';
import i18next from 'i18next';
import React from 'react';
import { Trans } from 'react-i18next';
import styled from 'styled-components';

import {
    getFieldDescription,
    getFieldOperatorDescription,
    getFieldParametersDescription,
    getFieldTransformDescription,
} from '@app/entityV2/shared/tabs/Dataset/Validations/fieldDescriptionUtils';

import { FieldAssertionInfo } from '@types';

type Props = {
    assertionInfo: FieldAssertionInfo;
    showColumnTag?: boolean;
    // use below description which is present in assertion info object to decide whether to show description or generate dynamic one
    assertionDescription?: Maybe<string>;
};

const StyledDescrptionContainer = styled.div`
    display: flex;
    flex-direction: column;
    gap: 4px;
`;
const StyledColumnTag = styled.div`
    align-items: center;
    background-color: ${(props) => props.theme.colors.bgSurface};
    width: fit-content;
    border-radius: 12px;
    height: 24px;
    display: flex;
    text-align: center;
    justify-content: center;
    padding: 0px 8px;
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
        components={{ bold: <Typography.Text style={{ fontWeight: 'bold' }} /> }}
    />
);

export const FieldAssertionDescription = ({ assertionInfo, showColumnTag, assertionDescription }: Props) => {
    const field = getFieldDescription(assertionInfo);
    let descriptionContent: React.ReactNode = assertionDescription;

    /* eslint-disable i18next/no-literal-string -- (untranslated-text) Inline prepositions and labels ('of', 'column', 'Values') assembled
       into description sentence; word order differs by language */
    if (!assertionDescription) {
        try {
            const transform = getFieldTransformDescription(assertionInfo);
            // Do not pluralize if this is a metric assertion since you're checking one metric, not multiple values
            const operator = getFieldOperatorDescription({ assertionInfo, isPlural: showColumnTag && !transform });
            const parameters = getFieldParametersDescription(assertionInfo);
            descriptionContent = (
                <>
                    {transform}
                    {transform ? ' of ' : ''}
                    {showColumnTag ? (
                        (transform && 'column') || 'Values'
                    ) : (
                        <Typography.Text style={{ fontWeight: 'bold' }}>{field}</Typography.Text>
                    )}{' '}
                    {operator} {parameters}
                </>
            );
        } catch (e) {
            // Helpers throw on unsupported enum values (e.g. operator = _NATIVE_ from external
            // integrations). Without this catch, the whole Assertions tab error-boundaries out.
            console.warn('Failed to render field assertion description', e);
            descriptionContent = renderFallbackDescription(field);
        }
    }
    /* eslint-enable i18next/no-literal-string */

    return (
        <StyledDescrptionContainer>
            <Typography.Text>{descriptionContent}</Typography.Text>
            {showColumnTag && <StyledColumnTag>{field}</StyledColumnTag>}
        </StyledDescrptionContainer>
    );
};
