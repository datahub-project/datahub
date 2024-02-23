import React from 'react';

import { Axis, BarSeries, BarStack, Grid, XYChart } from "@visx/xychart";
import { ParentSize } from "@visx/responsive";

import { Legend } from '../Legend';
import { ChartWrapper } from '../components';

export const HorizontalBarChart = <Data extends object, DataKeys>({
	data,
	dataKeys,
	xAccessor,
	yAccessor,
	colorAccessor
}: {
	data: Data[];
	dataKeys: DataKeys;
	yAccessor: (d: Data) => string;
	xAccessor: (d: any, key: string) => string;
	colorAccessor: (d: string) => string;
}) => {
	if (!Array.isArray(dataKeys)) throw new Error('Datakeys must be an array');

	const multipleData = data.length > 2;
	const margin = { top: 20, right: 0, bottom: 0, left: 100 };

	return (
		<ChartWrapper>
			<ParentSize>
				{({ width }) => {
					if (!width) return null;

					// Height of chart is variable on data length
					const baseHeight = 180;
					const height = data.length > 4 ? baseHeight + data.length * 10 : baseHeight;

					return (
						<>
							<Legend scale={colorAccessor} />
							<XYChart
								width={width}
								height={height}
								xScale={{ type: "linear" }}
								yScale={{ type: "band", paddingInner: 0.3 }}
								margin={margin}
							>
								<Grid numTicks={4} lineStyle={{ stroke: "#EAEAEA" }} rows={false} />
								{multipleData ? (
									<BarStack>
										{dataKeys.map((dK) => {
											return (
												<BarSeries
													key={dK}
													dataKey={dK}
													data={data}
													yAccessor={yAccessor}
													xAccessor={(d) => xAccessor(d, dK)}
													colorAccessor={() => colorAccessor(dK)}
												/>
											);
										})}
									</BarStack>
								) : (
									<BarSeries
										dataKey={dataKeys[0]}
										data={data}
										yAccessor={yAccessor}
										xAccessor={(d) => xAccessor(d, dataKeys[0])}
										colorAccessor={() => colorAccessor(dataKeys[0])}
										radiusTop
									/>
								)}
								<Axis
									orientation="left"
									tickLabelProps={{
										width: 50,
									}}
									hideAxisLine
								/>
							</XYChart>
						</>
					);
				}}
			</ParentSize>
		</ChartWrapper>
	);
};
