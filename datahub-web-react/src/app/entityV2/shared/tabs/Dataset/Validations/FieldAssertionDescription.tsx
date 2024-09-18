import React from 'react';
import { Typography } from 'antd';
import styled from 'styled-components';
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
 */
export const FieldAssertionDescription = ({ assertionInfo, showColumnTag }: Props) => {
    const field = getFieldDescription(assertionInfo);
    const operator = getFieldOperatorDescription({ assertionInfo, isPlural: showColumnTag });
    const transform = getFieldTransformDescription(assertionInfo);
    const parameters = getFieldParametersDescription(assertionInfo);

    const descriptionContent = (
        <>
            {transform}
            {transform ? ' of ' : ''}
            {showColumnTag ? (
                'column values'
            ) : (
                <Typography.Text style={{ fontWeight: 'bold' }}>{field}</Typography.Text>
            )}{' '}
            {operator} {parameters}
        </>
    );

    return (
        <StyledDescrptionContainer>
            <Typography.Text>{descriptionContent}</Typography.Text>
            {showColumnTag && <StyledColumnTag>{field}</StyledColumnTag>}
        </StyledDescrptionContainer>
    );
};
