import React from 'react';

import { useEntityContext } from '@app/entity/shared/EntityContext';
import { SUMMARY_DOMAIN_ICON_FONT_SIZE, SUMMARY_DOMAIN_ICON_SIZE } from '@app/entityV2/summary/properties/constants';
import BaseProperty from '@app/entityV2/summary/properties/property/properties/BaseProperty';
import { PropertyComponentProps } from '@app/entityV2/summary/properties/types';
import { DomainLink } from '@app/sharedV2/tags/DomainLink';

import { Domain } from '@types';

export default function DomainProperty(props: PropertyComponentProps) {
    const { entityData, loading } = useEntityContext();
    const domain = entityData?.domain?.domain;

    const renderTag = (tagAssociation: Domain) => {
        return (
            <DomainLink
                domain={tagAssociation}
                iconSize={SUMMARY_DOMAIN_ICON_SIZE}
                iconFontSize={SUMMARY_DOMAIN_ICON_FONT_SIZE}
            />
        );
    };

    return <BaseProperty {...props} values={domain ? [domain] : []} renderValue={renderTag} loading={loading} />;
}
