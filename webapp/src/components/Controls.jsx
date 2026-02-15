import React, { useState } from 'react';
import { Play, Square, Loader2, Settings, ChevronDown, ChevronUp } from 'lucide-react';

export default function Controls({ status, onStart, onStop }) {
    const isRunning = status === 'RUNNING';
    const [showSettings, setShowSettings] = useState(false);
    const [settings, setSettings] = useState({
        concurrency: 5,
        throttle: 0,
        retry_count: 3
    });

    const handleStart = (site) => {
        onStart(site, settings);
    };

    return (
        <div className="controls-container">
            <div className="controls">
                <div className="status-indicator">
                    Status: <span className={isRunning ? 'status-running' : 'status-idle'}>{status}</span>
                    {isRunning && <Loader2 className="spinner" size={16} />}
                </div>

                <div className="button-group">
                    <button onClick={() => handleStart('nasga')} disabled={isRunning}>
                        <Play size={16} /> Start Nasga
                    </button>
                    <button onClick={() => handleStart('heavyathlete')} disabled={isRunning}>
                        <Play size={16} /> Start Heavy
                    </button>
                    <button onClick={() => handleStart('scottishscores')} disabled={isRunning}>
                        <Play size={16} /> Start Scottish Scores
                    </button>
                    <button onClick={() => handleStart('all')} disabled={isRunning}>
                        <Play size={16} /> Start All
                    </button>

                    <button onClick={onStop} disabled={!isRunning} className="stop-btn">
                        <Square size={16} /> Stop
                    </button>
                </div>

                <button
                    className="settings-toggle"
                    onClick={() => setShowSettings(!showSettings)}
                >
                    <Settings size={16} /> Settings {showSettings ? <ChevronUp size={14} /> : <ChevronDown size={14} />}
                </button>
            </div>

            {showSettings && (
                <div className="settings-panel">
                    <div className="setting-item">
                        <label>Concurrency:</label>
                        <input
                            type="number"
                            value={settings.concurrency}
                            onChange={(e) => setSettings({ ...settings, concurrency: parseInt(e.target.value) || 1 })}
                            min="1" max="20"
                        />
                    </div>
                    <div className="setting-item">
                        <label>Throttle (ms):</label>
                        <input
                            type="number"
                            value={settings.throttle}
                            onChange={(e) => setSettings({ ...settings, throttle: parseInt(e.target.value) || 0 })}
                            min="0"
                        />
                    </div>
                    <div className="setting-item">
                        <label>Retries:</label>
                        <input
                            type="number"
                            value={settings.retry_count}
                            onChange={(e) => setSettings({ ...settings, retry_count: parseInt(e.target.value) || 0 })}
                            min="0"
                        />
                    </div>
                </div>
            )}
        </div>
    );
}
