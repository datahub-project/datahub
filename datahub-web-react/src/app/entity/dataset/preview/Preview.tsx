import React from 'react';
import { EntityType, FabricType, PlatformNativeType } from '../../../../types.generated';
import DefaultPreviewCard from '../../../preview/DefaultPreviewCard';
import { useEntityRegistry } from '../../../useEntityRegistry';

export const Preview = ({
    urn,
    name,
    origin,
    description,
    platformNativeType,
}: {
    urn: string;
    name: string;
    origin: FabricType;
    description?: string | null;
    platformNativeType?: PlatformNativeType | null;
}): JSX.Element => {
    const entityRegistry = useEntityRegistry();

    // TODO: Should we rename the search result card?
    return (
        <DefaultPreviewCard
            url={`/${entityRegistry.getPathName(EntityType.Dataset)}/${urn}`}
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
                    <b style={{ justifySelf: 'left' }}>Data Origin</b>
                    <div style={{ justifySelf: 'right' }}>{origin}</div>
                </div>
                <div
                    style={{
                        width: '150px',
                        margin: '5px 0px 5px 0px',
                        display: 'flex',
                        justifyContent: 'space-between',
                    }}
                >
                    <b>Platform</b>
                    <div>{platformNativeType}</div>
                </div>
            </>
        </DefaultPreviewCard>
    );
};
