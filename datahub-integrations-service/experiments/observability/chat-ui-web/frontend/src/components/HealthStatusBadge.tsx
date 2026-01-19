import type { ConversationHealthStatus } from '../api/types';
import { Warning, Question, XCircle, CheckCircle } from 'phosphor-react';

interface HealthStatusBadgeProps {
  healthStatus?: ConversationHealthStatus;
  compact?: boolean;
}

export function HealthStatusBadge({ healthStatus, compact = false }: HealthStatusBadgeProps) {
  if (!healthStatus) {
    return null;
  }

  const badges: JSX.Element[] = [];

  // Abandoned badge (highest priority - red)
  if (healthStatus.is_abandoned) {
    const reason = healthStatus.abandonment_reason;
    let title = 'Conversation abandoned';
    if (reason === 'no_response_at_all') {
      title = 'Bot never responded to last question';
    } else if (reason === 'incomplete_response') {
      title = 'Bot started but never finished response';
    }

    badges.push(
      <span key="abandoned" className="health-badge health-badge-abandoned" title={title}>
        {compact ? <Warning size={16} weight="regular" /> : <><Warning size={16} weight="regular" /> Abandoned</>}
      </span>
    );
  }

  // Error badge (medium priority - orange)
  if (healthStatus.has_errors) {
    badges.push(
      <span
        key="errors"
        className="health-badge health-badge-error"
        title="Conversation contains error messages or failed tool calls"
      >
        {compact ? <XCircle size={16} weight="regular" /> : <><XCircle size={16} weight="regular" /> Errors</>}
      </span>
    );
  }

  // Incomplete badge (low priority - yellow)
  if (
    !healthStatus.is_abandoned &&
    healthStatus.unanswered_questions_count > 0
  ) {
    const count = healthStatus.unanswered_questions_count;
    badges.push(
      <span
        key="incomplete"
        className="health-badge health-badge-incomplete"
        title={`${count} question${count > 1 ? 's' : ''} without complete responses`}
      >
        {compact ? <Question size={16} weight="regular" /> : <><Question size={16} weight="regular" /> {count} Incomplete</>}
      </span>
    );
  }

  // Completion rate indicator (show if < 100%)
  if (
    !healthStatus.is_abandoned &&
    healthStatus.completion_rate < 1.0 &&
    healthStatus.completion_rate > 0
  ) {
    const percentage = Math.round(healthStatus.completion_rate * 100);
    badges.push(
      <span
        key="completion"
        className="health-badge health-badge-completion"
        title={`${percentage}% of questions received complete responses`}
      >
        {compact ? `${percentage}%` : `${percentage}% Complete`}
      </span>
    );
  }

  // Success indicator (only if no issues)
  if (
    !healthStatus.is_abandoned &&
    !healthStatus.has_errors &&
    healthStatus.unanswered_questions_count === 0 &&
    healthStatus.completion_rate === 1.0
  ) {
    if (!compact) {
      badges.push(
        <span
          key="success"
          className="health-badge health-badge-success"
          title="All questions answered successfully"
        >
          <CheckCircle size={16} weight="regular" /> Complete
        </span>
      );
    }
  }

  if (badges.length === 0) {
    return null;
  }

  return <div className="health-status-badges">{badges}</div>;
}
