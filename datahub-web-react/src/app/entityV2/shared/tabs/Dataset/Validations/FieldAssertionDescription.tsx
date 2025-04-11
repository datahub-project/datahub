import React from 'react';
import { Typography } from 'antd';
import styled from 'styled-components';
import { Maybe } from 'graphql/jsutils/Maybe';
import { FieldAssertionInfo } from '../../../../../../types.generated';
import {
    getFieldDescription,
    getFieldOperatorDescription,
    getFieldParametersDescription,
    getFieldTransformDescription,
} from './fieldDescriptionUtils';
import { REDESIGN_COLORS } from '../../../constants';

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
    background-color: ${REDESIGN_COLORS.COLD_GREY_TEXT_BLUE_1};
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
export const FieldAssertionDescription = ({ assertionInfo, showColumnTag, assertionDescription }: Props) => {
    const field = getFieldDescription(assertionInfo);
    const transform = getFieldTransformDescription(assertionInfo);
    // Do not pluralize if this is a metric assertion since you're checking one metric, not multiple values
    const operator = getFieldOperatorDescription({ assertionInfo, isPlural: showColumnTag && !transform });
    const parameters = getFieldParametersDescription(assertionInfo);
    let descriptionContent = <>{assertionDescription}</>;

    if (!assertionDescription) {
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
    }

    return (
        <StyledDescrptionContainer>
            <Typography.Text>{descriptionContent}</Typography.Text>
            {showColumnTag && <StyledColumnTag>{field}</StyledColumnTag>}
        </StyledDescrptionContainer>
    );
};
