import { useState } from 'react';
import { useConfig } from './hooks/useConfig';
import { ChatWindow } from './components/ChatWindow';
import { SettingsTab } from './components/SettingsTab';
import { SearchTab } from './components/SearchTab';

type Tab = 'chat' | 'settings' | 'search';

export function App() {
  const [activeTab, setActiveTab] = useState<Tab>('chat');
  const {
    config,
    loading,
    error,
    updateConfig,
    testConnection,
    generateToken,
    ssoLogin,
    detectProfileForContext,
    listAwsProfiles,
    setupAwsProfile,
    validateAwsProfile,
  } = useConfig();

  return (
    <div className="app">
      <header className="app-header">
        <h1>DataHub AI Admin</h1>
        <div className="app-tabs">
          <button
            className={`tab ${activeTab === 'chat' ? 'active' : ''}`}
            onClick={() => setActiveTab('chat')}
          >
            Chat
          </button>
          <button
            className={`tab ${activeTab === 'search' ? 'active' : ''}`}
            onClick={() => setActiveTab('search')}
          >
            Search
          </button>
          <button
            className={`tab ${activeTab === 'settings' ? 'active' : ''}`}
            onClick={() => setActiveTab('settings')}
          >
            Settings
          </button>
        </div>
      </header>
      {error && <div className="app-error">{error}</div>}
      <main className="app-main">
        <div className={`tab-content ${activeTab === 'chat' ? 'active' : ''}`}>
          <ChatWindow config={config} />
        </div>
        <div className={`tab-content ${activeTab === 'settings' ? 'active' : ''}`}>
          <SettingsTab
            config={config}
            onUpdate={updateConfig}
            onTest={testConnection}
            onGenerateToken={generateToken}
            onSsoLogin={ssoLogin}
            onDetectProfile={detectProfileForContext}
            onListProfiles={listAwsProfiles}
            onSetupProfile={setupAwsProfile}
            onValidateProfile={validateAwsProfile}
            onSwitchToChat={() => setActiveTab('chat')}
            loading={loading}
          />
        </div>
        <div className={`tab-content ${activeTab === 'search' ? 'active' : ''}`}>
          <SearchTab />
        </div>
      </main>
    </div>
  );
}
