import { Modal } from '@components';
import React from 'react';

import RunDetailsContent from '@app/ingestV2/runDetails/RunDetailsContent';

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
            <RunDetailsContent urn={urn} setTitlePill={setTitlePill} />
        </Modal>
    );
};
