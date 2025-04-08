import { Divider } from 'antd';
import React from 'react';

import { useEntityData, useRefetch } from '@app/entity/shared/EntityContext';
import { ERModelRelationPreview } from '@app/entity/shared/components/styled/ERModelRelationship/ERModelRelationPreview';
import '@app/entity/shared/tabs/ERModelRelationship/ERModelRelationshipTab.less';

export const ERModelRelationshipTab = () => {
    const { entityData } = useEntityData();
    const refetch = useRefetch();
    const ermodelrelationView = (ermodelrelationData?: any): JSX.Element => {
        return (
            <ERModelRelationPreview
                ermodelrelationData={ermodelrelationData}
                prePageType="ERModelRelationship"
                refetch={refetch}
            />
        );
    };
    return (
        <>
            <div className="ERModelRelationshipTab">
                <div className="ermodelrelation-preview-div">{ermodelrelationView(entityData)}</div>
                <Divider className="thin-divider" />
            </div>
        </>
    );
};
