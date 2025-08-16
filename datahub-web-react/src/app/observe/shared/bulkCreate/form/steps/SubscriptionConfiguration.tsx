import React from 'react';

type Props = {
    subscriptionForm: React.ReactNode;
};

export const SubscriptionConfiguration = ({ subscriptionForm }: Props) => {
    return <div>{subscriptionForm}</div>;
};
