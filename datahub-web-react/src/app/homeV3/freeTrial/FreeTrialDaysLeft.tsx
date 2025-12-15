import { Badge, Button, Text } from '@components';
import React from 'react';
import styled from 'styled-components';

import { pluralize } from '@app/shared/textUtil';
import { useAppConfig } from '@app/useAppConfig';

const Container = styled.div`
    display: flex;
    gap: 12px;
    align-items: center;
    justify-content: center;
    margin-right: 8px;
    flex-wrap: wrap;
`;

const CONTACT_SALES_LINK = 'https://datahub.com/demo';

export default function FreeTrialDaysLeft() {
    const appConfig = useAppConfig();
    const days = appConfig.config.trialConfig.daysLeft;

    const onContactSales = () => {
        window.open(CONTACT_SALES_LINK, '_blank');
    };

    return (
        <Container>
            {days > 0 ? (
                <Text color="gray" colorLevel={600}>
                    Your trial ends in <Badge count={days} size="sm" color="violet" variant="version" />{' '}
                    {pluralize(days, 'day')}
                </Text>
            ) : (
                <Text color="gray" colorLevel={600}>
                    Your trial has ended
                </Text>
            )}
            <Button color="green" colorLevel={1000} size="sm" onClick={onContactSales}>
                Contact Sales
            </Button>
        </Container>
    );
}
