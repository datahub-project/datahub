import React, { useContext, useState, useEffect, ReactNode } from 'react';

import dayjs from 'dayjs';

import { useFormAnalyticsQuery } from '../../../graphql/analytics.generated';
import { sqlQueries } from './charts/queries';

// Define the time series options
const timeSeries = [
	{
		key: 7, // days count
		label: 'Last Week',
		tooltip: 'Forms assigned in the last week'
	},
	{
		key: 30,
		label: 'Last Month',
		tooltip: 'Forms assigned in the last month'
	},
	{
		key: 90,
		label: 'Last Quarter',
		tooltip: 'Forms assigned in the last quarter'
	},
	{
		key: 365,
		label: 'Last Year',
		tooltip: 'Forms assigned in the last year'
	}
];

// Define the context type
export interface FormAnalyticsContextType {
	sql: any;
	contextLoading: boolean;
	snapshot?: string;
	tabs: {
		selectedTab: string;
		setSelectedTab: (tab: string) => void;
	},
	timeSeries: {
		options: Array<{ key: number, label: string, tooltip: string }>;
		selectedSeries: number;
		setSeries: (series: number) => void;
	},
	byForm: {
		forms: any;
		hasForms: boolean;
		selectedForm?: string;
		setSelectedForm: (value: string) => void;
	},
	byAssignee: {
		assignees: any;
		hasAssignees: boolean;
		selectedAssignee?: string;
		setSelectedAssignee: (value: string) => void;
	},
	byDomain: {
		domains: any;
		hasDomains: boolean;
		selectedDomain?: string;
		setSelectedDomain: (value: string) => void;
	},
	sectionLoadStates: { // these are in order that they appear in the UI
		stats: boolean;
		overallProgress: boolean;
		forms: boolean;
		questions: boolean;
		assignees: boolean;
		domains: boolean;
		setLoadStates: (group: any, key: any, value: boolean) => void;
		resetLoadStates: () => void;
	}
}

// Default values
export const FormAnalyticsContext = React.createContext<FormAnalyticsContextType>({
	sql: {},
	contextLoading: true,
	snapshot: undefined,
	tabs: {
		selectedTab: '',
		setSelectedTab: () => null
	},
	timeSeries: {
		options: timeSeries,
		selectedSeries: 7,
		setSeries: () => null
	},
	byForm: {
		forms: {},
		hasForms: false,
		selectedForm: undefined,
		setSelectedForm: (_: string) => null
	},
	byAssignee: {
		assignees: {},
		hasAssignees: false,
		selectedAssignee: undefined,
		setSelectedAssignee: (_: string) => null
	},
	byDomain: {
		domains: {},
		hasDomains: false,
		selectedDomain: undefined,
		setSelectedDomain: (_: string) => null
	},
	sectionLoadStates: {
		stats: false,
		overallProgress: false,
		forms: false,
		questions: false,
		assignees: false,
		domains: false,
		setLoadStates: (_: any, __: any, ___: boolean) => null,
		resetLoadStates: () => null
	}
});

// Hook to use the context
export const useFormAnalyticsContext = () => useContext(FormAnalyticsContext);

// Provider Props
interface Props {
	children?: ReactNode | undefined;
}

// Provider component
export const FormAnalyticsProvider = ({ children }: Props) => {
	const [tab, setTab] = useState<string>('overall');
	const [series, setSeries] = useState<number>(7);
	const [selectedForm, setSelectedForm] = useState<string | undefined>();
	const [selectedAssignee, setSelectedAssignee] = useState<string | undefined>();
	const [selectedDomain, setSelectedDomain] = useState<string | undefined>();

	// Default waterfall load stats 
	const defaultLoadStates = {
		stats: {
			completedTrend: false,
			notStartedTrend: false
		},
		overallProgress: {
			assignedStatus: false,
			docProgress: false
		},
		forms: {
			progressByForm: false,
			formTopPerforming: false,
			formLeastPerforming: false
		},
		questions: {
			questions: false,
		},
		assignees: {
			progressByAssignee: false,
			assigneeTopPerforming: false,
			assigneeLeastPerforming: false
		},
		domains: {
			progressByDomain: false,
			domainTopPerforming: false,
			domainLeastPerforming: false
		}
	}

	// Waterfall render
	const [loadStates, setLoadStates] = useState<any>(defaultLoadStates);

	// Set the load states (to make updating DRY in components)
	const handleSetLoadStates = (group: any, key: any, value: boolean) => {
		setLoadStates(prevState => ({
			...prevState,
			[group]: {
				...prevState[group],
				[key]: value
			}
		}));
	};

	// Reset load states
	const resetLoadStates = () => setLoadStates(defaultLoadStates);

	// Handle switch the tabs 
	const handleSwitchTabs = (t: string) => {
		setTab(t);
		resetLoadStates();
	};

	// Handle switch the time series
	const handleSwitchSeries = (s: number) => {
		setSeries(s);
		resetLoadStates();
	}

	// Get date for queries from timeseries		
	const daysSinceDate = dayjs().subtract(series, 'day').format('YYYY-MM-DD');

	// Fetch max snapshot date
	const { data: snapshot, loading: snapshotLoading } = useFormAnalyticsQuery({
		variables: { input: { 'queryString': `select max(snapshot_date) from '{{ table }}'` } }
	});

	// Define sql queries
	const snapshotDate = snapshot?.formAnalytics?.table![0]?.row[0] as string;
	const sql = sqlQueries(daysSinceDate, selectedForm, selectedAssignee, selectedDomain, snapshotDate);

	// Fetch all the forms available 
	const { data: formsWithAnalytics, loading: formsWithAnalyticsLoading } = useFormAnalyticsQuery({
		variables: { input: { 'queryString': sql.getFormsWithAnalytics } },
		skip: snapshotLoading || !snapshotDate
	});

	// Fetch all the assigness available
	const { data: assignessWithFormAnalytics, loading: assignessWithFormAnalyticsLoading } = useFormAnalyticsQuery({
		variables: { input: { 'queryString': sql.getAssignessWithFormAnalytics } },
		skip: snapshotLoading || !snapshotDate
	});

	// Fetch all the domains available
	const { data: domainsWithFormAnalytics, loading: domainsWithFormAnalyticsLoading } = useFormAnalyticsQuery({
		variables: { input: { 'queryString': sql.getDomainsWithFormAnalytics } },
		skip: snapshotLoading || !snapshotDate
	});

	// Set the first form as selected if none is selected
	useEffect(() => {
		if (formsWithAnalytics?.formAnalytics?.table && formsWithAnalytics?.formAnalytics?.table?.length > 0 && !selectedForm) {
			const row = formsWithAnalytics?.formAnalytics?.table[0].row as any;
			if (row) setSelectedForm(row[0]); // form_id
		}
	}, [formsWithAnalytics?.formAnalytics?.table, selectedForm]);

	// Set the first assignee as selected if none is selected
	useEffect(() => {
		if (assignessWithFormAnalytics?.formAnalytics?.table && assignessWithFormAnalytics?.formAnalytics?.table?.length > 0 && !selectedAssignee) {
			const row = assignessWithFormAnalytics?.formAnalytics?.table[0].row as any;
			if (row) setSelectedAssignee(row[0]); // assignee_urn
		}
	}, [assignessWithFormAnalytics?.formAnalytics?.table, selectedAssignee]);

	// Set the first domain as selected if none is selected
	useEffect(() => {
		if (domainsWithFormAnalytics?.formAnalytics?.table && domainsWithFormAnalytics?.formAnalytics?.table?.length > 0 && !selectedDomain) {
			const row = domainsWithFormAnalytics?.formAnalytics?.table[0].row as any;
			if (row) setSelectedDomain(row[0]); // domain
		}
	}, [domainsWithFormAnalytics?.formAnalytics?.table, selectedDomain]);

	// Section load states (waterfall render)
	const sectionLoadStates = {
		stats:
			loadStates.stats.completedTrend
			&& loadStates.stats.notStartedTrend,
		overallProgress:
			loadStates.overallProgress.assignedStatus
			&& loadStates.overallProgress.docProgress,
		forms:
			loadStates.forms.progressByForm
			&& loadStates.forms.formTopPerforming
			&& loadStates.forms.formLeastPerforming,
		questions: loadStates.questions.questions,
		assignees:
			loadStates.assignees.progressByAssignee
			&& loadStates.assignees.assigneeTopPerforming
			&& loadStates.assignees.assigneeLeastPerforming,
		domains:
			loadStates.domains.progressByDomain
			&& loadStates.domains.domainTopPerforming
			&& loadStates.domains.domainLeastPerforming,
		setLoadStates: handleSetLoadStates,
		resetLoadStates,
	};

	// Return the context provider
	return (
		<FormAnalyticsContext.Provider value={{
			sql,
			contextLoading:
				snapshotLoading
				&& formsWithAnalyticsLoading
				&& assignessWithFormAnalyticsLoading
				&& domainsWithFormAnalyticsLoading,
			snapshot: snapshotDate,
			tabs: {
				selectedTab: tab,
				setSelectedTab: handleSwitchTabs
			},
			timeSeries: {
				options: timeSeries,
				selectedSeries: series,
				setSeries: handleSwitchSeries
			},
			byForm: {
				forms: formsWithAnalytics?.formAnalytics,
				hasForms: !formsWithAnalyticsLoading && formsWithAnalytics?.formAnalytics?.table ? formsWithAnalytics?.formAnalytics?.table?.length > 0 : false,
				selectedForm,
				setSelectedForm
			},
			byAssignee: {
				assignees: assignessWithFormAnalytics?.formAnalytics,
				hasAssignees: !assignessWithFormAnalyticsLoading && assignessWithFormAnalytics?.formAnalytics?.table ? assignessWithFormAnalytics?.formAnalytics?.table?.length > 0 : false,
				selectedAssignee,
				setSelectedAssignee
			},
			byDomain: {
				domains: domainsWithFormAnalytics?.formAnalytics,
				hasDomains: !domainsWithFormAnalyticsLoading && domainsWithFormAnalytics?.formAnalytics?.table ? domainsWithFormAnalytics?.formAnalytics?.table?.length > 0 : false,
				selectedDomain,
				setSelectedDomain
			},
			sectionLoadStates,
		}}>{children}</FormAnalyticsContext.Provider>
	);
};