import React from 'react';
import { Alert } from 'antd';

const CSV_FORMAT_LINK = 'https://datahubproject.io/docs/generated/ingestion/sources/csv-enricher';

export const CSVInfo = () => {
    const link = (
        <a href={CSV_FORMAT_LINK} target="_blank" rel="noopener noreferrer">
            link
        </a>
    );

    return (
        <Alert
            style={{ marginBottom: '10px' }}
            type="warning"
            banner
            message={
                <>
                    Add the URL of your CSV file to be ingested. This will work for any web-hosted CSV file. For
                    example, You can create a file in google sheets following the format at this {link} and then
                    construct the CSV URL by publishing your google sheet in the CSV format.
                </>
            }
        />
    );
};
