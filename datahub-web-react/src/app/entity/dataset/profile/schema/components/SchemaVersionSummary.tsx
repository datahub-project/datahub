import React from 'react';
import { Typography } from 'antd';
import styled from 'styled-components';

const SummaryContainer = styled.div`
    margin-bottom: 16px;
    padding-left: 10px;
    & ul {
        padding-inline-start: 30px;
        margin-top: 5px;
    }
`;

type Props = {
    diffSummary: {
        added: number;
        removed: number;
        updated: number;
    };
};

export default function SchemaVersionSummary({ diffSummary }: Props) {
    return (
        <SummaryContainer>
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
