/* eslint-disable */
// TODO: Cleanup linting errors

import React, { useState, useEffect, useRef } from 'react';

import { Input } from 'antd';

import {
	Step,
	StepHeader,
	StepField,
	StepButtons,
} from './components';

import { SecondaryButton } from '../sharedComponents';

import { getSteps } from './utils';

import { TermSelector } from './fields/TermSelector';
import { ConnectionSelector } from './fields/ConnectionSelector';
import { CategorySelector } from './fields/CategorySelector';
import { TraversalSelector } from './fields/TraversalSelector';
import { CustomActionSelector } from './fields/CustomActionSelector';
import { DataAssetSelector } from './fields/DataAssetSelector';
import { ConditionSelector } from './fields/ConditionSelector';

export const Configure = ({ automation, formData, setFormData }: any) => {
	const steps = getSteps(automation);
	const prevProps = useRef(formData);

	// Various field states
	const [assetTypesSelected, setAssetTypesSelected] = useState<any[]>([]);
	const [termsSelected, setTermsSelected] = useState<any[]>([]);
	const [connectionSelected, setConnectionSelected] = useState<string>();
	const [categorySelected, setCategorySelected] = useState<string>();
	const [details, setDetails] = useState<any>({});

	// Form Data to be submitted
	const data = {
		termsSelected,
		connectionSelected,
		categorySelected,
		details
	};

	// Send the form data back to the parent component
	// Only sends the data if the form data has changed
	useEffect(() => {
		if (JSON.stringify(prevProps.current) !== JSON.stringify(data)) {
			setFormData(data);
		}
		prevProps.current = data;
	}, [data]);

	return (
		<div>
			{steps.map((step: any, index: number) => (
				<Step key={index}>
					{/* Header */}
					<StepHeader>
						<h2>{step.title}</h2>
						<p>{step.description}</p>
					</StepHeader>

					{/* Fields */}
					{step.fields.map((field: any, index: number) => (
						<StepField key={index}>
							{/* Field Label */}
							{field.label && (
								<label>
									{field.label}
									{field.isRequired && <sup>*</sup>}
								</label>
							)}

							{/* Term Selector */}
							{field.type === 'termSelector' && (
								<TermSelector
									termsSelected={termsSelected}
									setTermsSelected={setTermsSelected}
									isRequired={field.isRequired}
								/>
							)}

							{/* Connection Selector */}
							{field.type === 'connectionSelector' && (
								<ConnectionSelector
									connectionTypes={step.connectionTypes}
									connectionSelected={connectionSelected}
									setConnectionSelected={setConnectionSelected}
									isRequired={field.isRequired}
								/>
							)}

							{/* Traversal Selector */}
							{field.type === 'traversalSelector' && (
								<TraversalSelector />
							)}

							{/* Custom Actions */}
							{field.type === 'customActionSelector' && (
								<CustomActionSelector />
							)}

							{/* Data Asset Selector */}
							{field.type === 'dataAssetSelector' && (
								<DataAssetSelector
									dataAssetSelected={assetTypesSelected}
									setDataAssetSelected={setAssetTypesSelected}
								/>
							)}

							{/* Condition Selector */}
							{field.type === 'conditionSelector' && (
								<ConditionSelector
									selectedAssetTypes={assetTypesSelected}
								/>
							)}

							{/* Category Selector */}
							{field.type === 'categorySelector' && (
								<CategorySelector
									categorySelected={categorySelected}
									setCategorySelected={setCategorySelected}
									isRequired={field.isRequired}
								/>
							)}

							{/* Text Input */}
							{field.type === 'text' && (
								<Input
									type="text"
									onChange={(e) => setDetails({ ...details, [field.label.toLowerCase()]: e.target.value })}
									required={field.isRequired}
								/>
							)}

							{/* Text Area */}
							{field.type === 'longtext' && (
								<Input.TextArea
									onChange={(e) => setDetails({ ...details, [field.label.toLowerCase()]: e.target.value })}
									required={field.isRequired}
								/>
							)}
						</StepField>
					))}

					{/* Test / Preview Buttons */}
					{step.canTest || step.canPreview && (
						<StepButtons>
							{step.canTest && <SecondaryButton disabled>{step.testTitle}</SecondaryButton>}
							{step.canPreview && <SecondaryButton disabled>{step.previewTitle}</SecondaryButton>}
						</StepButtons>
					)}
				</Step>
			))}
		</div>
	);
}