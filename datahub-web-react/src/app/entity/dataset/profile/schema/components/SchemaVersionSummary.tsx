/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';

const SummaryContainer = styled.div`
    margin-bottom: 16px;
    padding-left: 10px;
    & ul {
        padding-inline-start: 30px;
        margin-top: 5px;
    }
`;

export interface SchemaDiffSummary {
    added: number;
    removed: number;
    updated: number;
}

type Props = {
    diffSummary: SchemaDiffSummary;
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
