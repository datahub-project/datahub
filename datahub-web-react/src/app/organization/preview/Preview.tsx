import { BankOutlined } from '@ant-design/icons';
import React from 'react';

import DefaultPreviewCard from '@app/preview/DefaultPreviewCard';
import { EntityType, Owner } from '@app/types.generated';
import { useEntityRegistry } from '@app/useEntityRegistry';

interface Props {
    urn: string;
    name: string;
    description?: string | null;
    owners?: Array<Owner> | null;
    logoComponent?: JSX.Element;
}

export const Preview = ({ urn, name, description, owners, logoComponent }: Props) => {
    const entityRegistry = useEntityRegistry();
    return (
        <DefaultPreviewCard
            url={entityRegistry.getEntityUrl(EntityType.Organization, urn)}
            name={name || ''}
            urn={urn}
            description={description || ''}
            type="Organization"
            typeIcon={<BankOutlined style={{ fontSize: 14, color: '#BFBFBF' }} />}
            owners={owners}
            logoComponent={logoComponent}
        />
    );
};
