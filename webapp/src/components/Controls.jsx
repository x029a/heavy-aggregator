import React from 'react';
import { Play, Square, Loader2 } from 'lucide-react';

export default function Controls({ status, onStart, onStop }) {
    const isRunning = status === 'RUNNING';

    return (
        <div className="controls">
            <div className="status-indicator">
                Status: <span className={isRunning ? 'status-running' : 'status-idle'}>{status}</span>
                {isRunning && <Loader2 className="spinner" size={16} />}
            </div>

            <div className="button-group">
                <button onClick={() => onStart('nasga')} disabled={isRunning}>
                    <Play size={16} /> Start Nasga
                </button>
                <button onClick={() => onStart('heavyathlete')} disabled={isRunning}>
                    <Play size={16} /> Start Heavy
                </button>
                <button onClick={() => onStart('scottishscores')} disabled={isRunning}>
                    <Play size={16} /> Start Scottish
                </button>
                <button onClick={() => onStart('all')} disabled={isRunning}>
                    <Play size={16} /> Start All
                </button>

                <button onClick={onStop} disabled={!isRunning} className="stop-btn">
                    <Square size={16} /> Stop
                </button>
            </div>
        </div>
    );
}
