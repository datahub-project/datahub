import React from 'react';
import { Typography } from 'antd';
import styled from 'styled-components';

const SummaryContainer = styled.div`
    margin-top: -32px;
    margin-bottom: 16px;
    padding-left: 10px;
    & ul {
        padding-inline-start: 30px;
        margin-top: 5px;
    }
`;
const SummarySubHeader = styled(Typography.Text)``;

type Props = {
    diffSummary: {
        added: number;
        removed: number;
        updated: number;
    };
    currentVersion: number;
};

export default function SchemaVersionSummary({ diffSummary, currentVersion }: Props) {
    return (
        <SummaryContainer>
            <Typography.Title level={5}>Summary</Typography.Title>
            <SummarySubHeader>{`Comparing version ${currentVersion} to version ${
                currentVersion - 1
            }`}</SummarySubHeader>
            <ul>
                {diffSummary.added ? (
                    <li>
                        <Typography.Text>{`${diffSummary.added} column${
                            diffSummary.added > 1 ? 's were' : ' was'
                        } added`}</Typography.Text>
                    </li>
                ) : null}
                {diffSummary.removed ? (
                    <li>
                        <Typography.Text>{`${diffSummary.removed} column${
                            diffSummary.removed > 1 ? 's were' : ' was'
                        } removed`}</Typography.Text>
                    </li>
                ) : null}
                {diffSummary.updated ? (
                    <li>
                        <Typography.Text>{`${diffSummary.updated} description${
                            diffSummary.updated > 1 ? 's were' : ' was'
                        } updated`}</Typography.Text>
                    </li>
                ) : null}
            </ul>
        </SummaryContainer>
    );
}
