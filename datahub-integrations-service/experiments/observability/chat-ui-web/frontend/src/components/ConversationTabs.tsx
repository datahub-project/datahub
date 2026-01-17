interface ConversationTabsProps {
  activeTab: 'active' | 'archived';
  onTabChange: (tab: 'active' | 'archived') => void;
  activeCount: number;
  archivedTotal: number;
}

export function ConversationTabs({
  activeTab,
  onTabChange,
  activeCount,
  archivedTotal,
}: ConversationTabsProps) {
  return (
    <div className="conversation-tabs">
      <button
        className={`tab ${activeTab === 'active' ? 'active' : ''}`}
        onClick={() => onTabChange('active')}
      >
        Active Chats
        <span className="count-badge">{activeCount}</span>
      </button>
      <button
        className={`tab ${activeTab === 'archived' ? 'active' : ''}`}
        onClick={() => onTabChange('archived')}
      >
        History
        <span className="count-badge">{archivedTotal}</span>
      </button>
    </div>
  );
}
