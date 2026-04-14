import { Modal } from '@components';
import React from 'react';

import RunDetailsContent from '@app/ingestV2/runDetails/RunDetailsContent';

import { useGetIngestionExecutionRequestQuery } from '@graphql/ingestion.generated';

const modalBodyStyle = {
    padding: 0,
    height: '80vh',
};

type Props = {
    urn: string;
    open: boolean;
    onClose: () => void;
};

export const ExecutionDetailsModal = ({ urn, open, onClose }: Props) => {
    const [titlePill, setTitlePill] = React.useState<React.ReactNode>(null);
    const { data, loading, error, refetch } = useGetIngestionExecutionRequestQuery({ variables: { urn } });

    return (
        <Modal
            width="1400px"
            bodyStyle={modalBodyStyle}
            title="Run Details"
            titlePill={titlePill}
            open={open}
            onCancel={onClose}
            buttons={[]}
        >
            <RunDetailsContent
                urn={urn}
                data={data}
                refetch={refetch}
                loading={loading}
                error={error}
                setTitlePill={setTitlePill}
            />
        </Modal>
    );
};
