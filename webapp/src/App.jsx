import React, { useState, useEffect } from 'react';
import Controls from './components/Controls';
import Terminal from './components/Terminal';
import FileBrowser from './components/FileBrowser';
import DataViewer from './components/DataViewer';
import { Activity, Database, Server } from 'lucide-react';
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

  const startScraper = (site) => {
    fetch('/api/scrape', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ site })
    }).catch(err => console.error(err));
  };

  const stopScraper = () => {
    fetch('/api/stop', { method: 'POST' }).catch(err => console.error(err));
  };

  return (
    <div className="app-container">
      <nav className="sidebar">
        <div className="logo">
          <Server size={24} /> HeavyAgg
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
