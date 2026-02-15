import React, { useEffect, useRef, useState } from 'react';

export default function Terminal() {
    const [logs, setLogs] = useState([]);
    const endRef = useRef(null);

    useEffect(() => {
        const protocol = window.location.protocol === 'https:' ? 'wss' : 'ws';
        const ws = new WebSocket(`${protocol}://${window.location.host}/api/logs`);

        ws.onmessage = (event) => {
            setLogs((prev) => [...prev, event.data]);
        };

        ws.onclose = () => console.log('WebSocket disconnected');

        return () => ws.close();
    }, []);

    useEffect(() => {
        endRef.current?.scrollIntoView({ behavior: 'smooth' });
    }, [logs]);

    return (
        <div className="terminal">
            {logs.map((log, i) => (
                <div key={i} className="log-line">{log}</div>
            ))}
            <div ref={endRef} />
        </div>
    );
}
