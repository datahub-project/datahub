import { useState } from 'react';
import { useConfig } from './hooks/useConfig';
import { ChatWindow } from './components/ChatWindow';
import { SettingsTab } from './components/SettingsTab';

type Tab = 'chat' | 'settings';

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
  } = useConfig();

  return (
    <div className="app">
      <header className="app-header">
        <h1>DataHub Chat Admin</h1>
        <div className="app-tabs">
          <button
            className={`tab ${activeTab === 'chat' ? 'active' : ''}`}
            onClick={() => setActiveTab('chat')}
          >
            Chat
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
        {activeTab === 'chat' && <ChatWindow />}
        {activeTab === 'settings' && (
          <SettingsTab
            config={config}
            onUpdate={updateConfig}
            onTest={testConnection}
            onGenerateToken={generateToken}
            onSsoLogin={ssoLogin}
            loading={loading}
          />
        )}
      </main>
    </div>
  );
}
