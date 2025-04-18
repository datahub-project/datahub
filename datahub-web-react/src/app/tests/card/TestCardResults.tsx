import React from 'react';

import { TestResultsSummary } from '@app/tests/TestResultsSummary';

type Props = {
    urn: string;
    name: string;
    editVersion: number;
};

export const TestCardResults = (props: Props) => {
    return <TestResultsSummary {...props} />;
};
