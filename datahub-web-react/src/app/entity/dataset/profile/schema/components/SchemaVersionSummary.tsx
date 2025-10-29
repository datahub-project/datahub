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
