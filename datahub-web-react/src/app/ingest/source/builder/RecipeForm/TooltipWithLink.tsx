import React from 'react';

export const TooltipWithLink = () => {
    return (
        <div>
            <p>
                Which table lineage collector mode to use. Check out{' '}
                <a
                    href="https://datahubproject.io/docs/generated/ingestion/sources/redshift/#config-details"
                    target="_blank"
                    rel="noreferrer"
                >
                    the documentation
                </a>{' '}
                explaining the difference between the three available modes.
            </p>
        </div>
    );
};
