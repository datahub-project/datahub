/*
* Resuable Term Selector Component
* Please keep this agnostic and reusable
*/

import React from 'react';

import { SnowflakeConnectionSelector } from '../../../../settings/platform/snowflake/ConnectionSelector';

export const ConnectionSelector = ({ setConnectionSelected }: any) => (
	<SnowflakeConnectionSelector handleChange={(value) => setConnectionSelected(value)} />
);