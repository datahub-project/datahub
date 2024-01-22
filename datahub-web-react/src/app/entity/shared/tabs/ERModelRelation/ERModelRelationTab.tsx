import React from 'react';
import { Divider } from 'antd';
import './ERModelRelationTab.less';
import { ERModelRelationPreview } from '../../components/styled/ERModelRelation/ERModelRelationPreview';
import { useEntityData, useRefetch } from '../../EntityContext';

export const ERModelRelationTab = () => {
    const { entityData } = useEntityData();
    const refetch = useRefetch();
    const ermodelrelationView = (ermodelrelationData?: any): JSX.Element => {
        return <ERModelRelationPreview ermodelrelationData={ermodelrelationData} prePageType="ERModelRelation" refetch={refetch} />;
    };
    return (
        <>
            <div className="ERModelRelationTab">
                <div className="ermodelrelation-preview-div">{ermodelrelationView(entityData)}</div>
                <Divider className="thin-divider" />
            </div>
        </>
    );
};
