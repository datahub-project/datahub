import React from 'react';
import { EntityType } from '../../../../types.generated';
import DefaultPreviewCard from '../../../preview/DefaultPreviewCard';
import { useEntityRegistry } from '../../../useEntityRegistry';

export const ChartPreview = ({
    urn,
    name,
    description,
    platform,
}: {
    urn: string;
    platform: string;
    name?: string;
    description?: string | null;
}): JSX.Element => {
    const entityRegistry = useEntityRegistry();

    return (
        <DefaultPreviewCard
            url={`/${entityRegistry.getPathName(EntityType.Chart)}/${urn}`}
            title={<div style={{ margin: '5px 0px 5px 2px', fontSize: '20px', fontWeight: 'bold' }}>{name}</div>}
        >
            <>
                <div style={{ margin: '0px 0px 15px 0px' }}>{description}</div>
                <div
                    style={{
                        width: '150px',
                        margin: '5px 0px 5px 0px',
                        display: 'flex',
                        justifyContent: 'space-between',
                    }}
                >
                    <b style={{ justifySelf: 'left' }}>Platform</b>
                    <div style={{ justifySelf: 'right' }}>{platform}</div>
                </div>
            </>
        </DefaultPreviewCard>
    );
};
