import { Priority } from '@components/components/IncidentPriorityLabel/constant';

export interface IncidentPriorityLabelProps {
    priority: Priority;
    title: string;
    style?: React.CSSProperties;
}
