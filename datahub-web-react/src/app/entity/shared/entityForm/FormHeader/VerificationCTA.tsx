import { LoadingOutlined } from '@ant-design/icons';
import Icon from '@ant-design/icons/lib/components/Icon';
import { Button } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { FormView, useEntityFormContext } from '@app/entity/shared/entityForm/EntityFormContext';
import { pluralize } from '@app/shared/textUtil';

import PurpleVerificationLogo from '@images/verificationPurpleWhite.svg?react';

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
    const {
        form: { setFormView },
        entity: { setSelectedEntities },
        counts: {
            verificationType: { verifyReady },
        },
        submission: { nonOptimisticLoading, verificationDataLoading },
    } = useEntityFormContext();

    function goToBulkVerify() {
        setFormView(FormView.BULK_VERIFY);
        setSelectedEntities([]);
    }

    return (
        <StyledButton onClick={goToBulkVerify}>
            <StyledIcon component={PurpleVerificationLogo} />
            {nonOptimisticLoading || verificationDataLoading ? <LoadingOutlined /> : verifyReady}{' '}
            {pluralize(verifyReady, 'asset')}
            <br />
            ready for verification {'->'}
        </StyledButton>
    );
}
