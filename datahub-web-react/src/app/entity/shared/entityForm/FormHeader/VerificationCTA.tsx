import { Button } from 'antd';
import Icon from '@ant-design/icons/lib/components/Icon';
import React from 'react';
import styled from 'styled-components';
import { ReactComponent as PurpleVerificationLogo } from '../../../../../images/verificationPurpleWhite.svg';
import { FormView, useEntityFormContext } from '../EntityFormContext';
import { pluralize } from '../../../../shared/textUtil';

const StyledButton = styled(Button)`
    margin-top: 16px;
    font-size: 16px;
    max-width: 240px;
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    justify-content: center;
    height: min-content;
    color: white;
    background-color: black;
    border: none;
    &:hover,
    &:focus {
        color: white;
        background-color: black;
        border: none;
    }
`;

const StyledIcon = styled(Icon)`
    height: 18px;
    width: 18px;
    margin-right: -4px;

    svg {
        height: 18px;
        width: 18px;
    }
`;

export default function VerificationCTA() {
    const { numReadyForVerification, setFormView, setSelectedEntities, isVerificationType } = useEntityFormContext();

    if (!isVerificationType || numReadyForVerification <= 0) return null;

    function goToBulkVerify() {
        setFormView(FormView.BULK_VERIFY);
        setSelectedEntities([]);
    }

    return (
        <StyledButton onClick={goToBulkVerify}>
            <StyledIcon component={PurpleVerificationLogo} />
            {numReadyForVerification} {pluralize(numReadyForVerification, 'asset')}
            <br />
            eligible for verification {'->'}
        </StyledButton>
    );
}
