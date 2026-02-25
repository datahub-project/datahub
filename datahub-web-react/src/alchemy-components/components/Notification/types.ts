import { ArgsProps } from 'antd/lib/notification';

export enum NotificationType {
    SUCCESS = 'success',
    ERROR = 'error',
    INFO = 'info',
    WARNING = 'warning',
}

export type NotificationProps = Pick<ArgsProps, 'message' | 'description' | 'duration' | 'placement' | 'onClose'>;
