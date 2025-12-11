/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Alert } from 'antd';
import React from 'react';

const CSV_FORMAT_LINK = 'https://docs.datahub.com/docs/generated/ingestion/sources/csv-enricher';

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
