import React from 'react';

import { Button, Tooltip } from 'antd';
import { SeriesContainer } from './components';
import { ANTD_GRAY } from '../../entity/shared/constants';

import { useFormAnalyticsContext } from './FormAnalyticsContext';

export const SeriesSelect = () => {
	const { timeSeries: { options, setSeries, selectedSeries } } = useFormAnalyticsContext();
	const isActive = (key: number) => selectedSeries === key;

	return (
		<SeriesContainer>
			{options.map((s) => (
				<Tooltip title={s.tooltip} showArrow={false} placement="top" >
					<Button
						size="small"
						onClick={() => setSeries(s.key)}
						style={{
							background: isActive(s.key) ? '#11ADA0' : 'white',
							borderColor: isActive(s.key) ? '#11ADA0' : ANTD_GRAY[6],
							color: isActive(s.key) ? 'white' : 'inherit'
						}}
					>
						{s.label}
					</Button>
				</Tooltip>
			))
			}
		</SeriesContainer >
	);
}