import { Link } from '@components';
import React from 'react';

export default function SelectSourceSubtitle() {
    return (
        <>
            Select a platform to connect to DataHub.{' '}
            <Link href="https://docs.datahub.com/docs/metadata-ingestion-security" style={{ fontStyle: 'italic' }}>
                Learn more about keeping credentials in your environment.
            </Link>
        </>
    );
}
