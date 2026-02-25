import { colors } from '@components';
import { Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { ExecutionRequestRecord } from '@app/ingestV2/executions/types';
import { NameColumn } from '@app/ingestV2/source/IngestionSourceTableColumns';

const TextContainer = styled(Typography.Text)`
    color: ${colors.gray[1700]};
`;

interface Props {
    record: ExecutionRequestRecord;
    navigateToSource: () => void;
}

export default function SourceColumn({ record, navigateToSource }: Props) {
    if (record.type && record.name) {
        return <NameColumn type={record.type} record={record} onNameClick={navigateToSource} />;
    }

    return (
        <TextContainer
            ellipsis={{
                tooltip: {
                    title: record.name,
                    color: 'white',
                    overlayInnerStyle: { color: colors.gray[1700] },
                    showArrow: false,
                },
            }}
        >
            Deleted source
        </TextContainer>
    );
}
