import { Modal, message } from 'antd';
import React, { useEffect, useState } from 'react';
import styled from 'styled-components';

import { ExecutionDetailsModal } from '@app/ingest/source/executions/ExecutionRequestDetailsModal';
import IngestionExecutionTable from '@app/ingest/source/executions/IngestionExecutionTable';
import useRefreshIngestionData from '@app/ingest/source/executions/useRefreshIngestionData';
import { ROLLING_BACK, RUNNING } from '@app/ingest/source/utils';
import { Message } from '@app/shared/Message';
import { SearchCfg } from '@src/conf';

import {
    useCancelIngestionExecutionRequestMutation,
    useGetIngestionSourceQuery,
    useRollbackIngestionMutation,
} from '@graphql/ingestion.generated';
import { ExecutionRequest } from '@types';

const ListContainer = styled.div`
    margin-left: 28px;
`;

export function isExecutionRequestActive(executionRequest: ExecutionRequest) {
    return executionRequest.result?.status === RUNNING || executionRequest.result?.status === ROLLING_BACK;
}

type Props = {
    urn: string;
    isExpanded: boolean;
    lastRefresh: number;
    onRefresh: () => void;
};
