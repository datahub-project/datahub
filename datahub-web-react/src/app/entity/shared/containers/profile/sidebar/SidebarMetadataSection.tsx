import React from 'react';

import { useAppConfig } from '../../../../../useAppConfig';
import { useEntityData } from '../../../EntityContext';

import { SharedEntityInfo } from './SharedEntityInfo';

export const SidebarMetadataSection = () => {
	const appConfig = useAppConfig();
	const { entityData } = useEntityData();

	// Hide if flag disabled or user lacks permissions
	const { metadataShareEnabled } = appConfig.config.featureFlags;
	const canShareEntity = entityData?.privileges?.canShareEntity;
	if (!metadataShareEnabled || !canShareEntity) return null;

	// Get latest share data 
	const lastShareResults = entityData?.share?.lastShareResults;

	// Hide if no share result 
	if (!lastShareResults || lastShareResults?.length === 0 || !lastShareResults[0]) return null;

	return <SharedEntityInfo lastShareResults={lastShareResults} />;
}