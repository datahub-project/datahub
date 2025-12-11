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

import { LOOKER, LOOK_ML } from '@app/ingest/source/builder/constants';

const LOOKML_DOC_LINK = 'https://docs.datahub.com/docs/generated/ingestion/sources/looker#module-lookml';
const LOOKER_DOC_LINK = 'https://docs.datahub.com/docs/generated/ingestion/sources/looker#module-looker';

interface Props {
    type: string;
}

export const LookerWarning = ({ type }: Props) => {
    let link: React.ReactNode;
    if (type === LOOKER) {
        link = (
            <a href={LOOKML_DOC_LINK} target="_blank" rel="noopener noreferrer">
                DataHub LookML Ingestion Source
            </a>
        );
    } else if (type === LOOK_ML) {
        link = (
            <a href={LOOKER_DOC_LINK} target="_blank" rel="noopener noreferrer">
                DataHub Looker Ingestion Source
            </a>
        );
    }

    return (
        <Alert
            style={{ marginBottom: '10px' }}
            type="warning"
            banner
            message={
                <>
                    To complete the Looker integration (including Looker views and lineage to the underlying warehouse
                    tables), you must <b>also</b> use the {link}.
                </>
            }
        />
    );
};
