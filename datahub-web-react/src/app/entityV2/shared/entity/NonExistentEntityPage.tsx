import { Result } from 'antd';
import React from 'react';

export default function NonExistentEntityPage() {
    return <Result status="404" title="Not Found" subTitle="Sorry, we are unable to find this entity in DataHub" />;
}
