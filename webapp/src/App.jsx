import React, { useState, useEffect } from 'react';
import Controls from './components/Controls';
import Terminal from './components/Terminal';
import FileBrowser from './components/FileBrowser';
import DataViewer from './components/DataViewer';
import { Activity, Database, Server, Square } from 'lucide-react';
import './App.css';

function App() {
  const [activeTab, setActiveTab] = useState('dashboard');
  const [status, setStatus] = useState('IDLE');
  const [selectedFile, setSelectedFile] = useState(null);

  useEffect(() => {
    // Poll status
    const interval = setInterval(() => {
      fetch('/api/status')
        .then(res => res.json())
        .then(data => setStatus(data.status))
        .catch(err => console.error("Status fetch error:", err));
    }, 2000);
    return () => clearInterval(interval);
  }, []);

  const startScraper = (site, settings) => {
    fetch('/api/scrape', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ site, settings })
    }).catch(err => console.error(err));
  };

  const stopScraper = () => {
    fetch('/api/stop', { method: 'POST' }).catch(err => console.error(err));
  };

  const shutdownServer = () => {
    if (confirm("Are you sure you want to shut down the server?")) {
      fetch('/api/shutdown', { method: 'POST' })
        .then(() => alert("Server shutting down..."))
        .catch(err => console.error(err));
    }
  };

  return (
    <div className="app-container">
      <nav className="sidebar">
        <div className="logo">
          <Server size={24} /> Heavy Aggregator
        </div>
        <button
          className={`nav-item ${activeTab === 'dashboard' ? 'active' : ''}`}
          onClick={() => setActiveTab('dashboard')}
        >
          <Activity size={20} /> Dashboard
        </button>
        <button
          className={`nav-item ${activeTab === 'files' ? 'active' : ''}`}
          onClick={() => setActiveTab('files')}
        >
          <Database size={20} /> Data Explorer
        </button>

        <div className="spacer" style={{ flex: 1 }}></div>

        <button className="nav-item shutdown-btn" onClick={shutdownServer}>
          <div style={{ display: 'flex', alignItems: 'center', gap: '10px', color: '#ff4444' }}>
            <Square size={16} fill="currentColor" /> Shutdown Server
          </div>
        </button>
      </nav>

      <main className="content">
        {activeTab === 'dashboard' && (
          <div className="dashboard-view">
            <header>
              <h1>Scraper Dashboard</h1>
            </header>
            <Controls status={status} onStart={startScraper} onStop={stopScraper} />
            <Terminal />
          </div>
        )}

        {activeTab === 'files' && (
          <div className="files-view">
            <header>
              <h1>Data Explorer</h1>
            </header>
            <div className="explorer-layout">
              <div className="explorer-sidebar">
                <FileBrowser onSelect={(path) => setSelectedFile(path)} />
              </div>
              <div className="explorer-content">
                <DataViewer filePath={selectedFile} />
              </div>
            </div>
          </div>
        )}
      </main>
    </div>
  );
}

export default App;
