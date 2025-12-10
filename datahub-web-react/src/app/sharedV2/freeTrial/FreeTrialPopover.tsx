/**
 * Re-export PopoverCard as FreeTrialPopover for backwards compatibility.
 * New code should import PopoverCard from '@app/sharedV2/popoverCard' directly.
 */
export { default, default as FreeTrialPopover } from '../popoverCard/PopoverCard';
export type { PopoverCardProps as FreeTrialPopoverProps, PopoverPosition } from '../popoverCard/PopoverCard';
