import React from 'react';
import { Alert } from 'antd';
import { LOOKER, LOOK_ML } from './constants';

const LOOKML_DOC_LINK = 'https://datahubproject.io/docs/generated/ingestion/sources/looker#module-lookml';
const LOOKER_DOC_LINK = 'https://datahubproject.io/docs/generated/ingestion/sources/looker#module-looker';

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
