import React from 'react';
import { Divider } from 'antd';
import './ERModelRelationshipTab.less';
import { ERModelRelationPreview } from '../../components/styled/ERModelRelationship/ERModelRelationPreview';
import { useEntityData, useRefetch } from '../../EntityContext';

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
