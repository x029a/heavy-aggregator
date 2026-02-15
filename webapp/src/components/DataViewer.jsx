import React, { useState, useEffect } from 'react';

export default function DataViewer({ filePath }) {
    const [content, setContent] = useState(null);
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState(null);

    useEffect(() => {
        if (!filePath) {
            setContent(null);
            return;
        }

        setLoading(true);
        fetch(`/api/files/content?path=${filePath}`)
            .then(res => {
                if (!res.ok) throw new Error("Failed to load file");
                return res.text();
            })
            .then(text => {
                try {
                    const json = JSON.parse(text);
                    setContent(json);
                    setError(null);
                } catch {
                    setContent(text); // Fallback to text
                    setError(null);
                }
            })
            .catch(err => setError(err.message))
            .finally(() => setLoading(false));
    }, [filePath]);

    if (!filePath) return <div className="viewer-placeholder">Select a file to view content</div>;
    if (loading) return <div className="viewer-loading">Loading...</div>;
    if (error) return <div className="viewer-error">{error}</div>;

    return (
        <div className="data-viewer">
            <h3>{filePath}</h3>
            <div className="content-scroll">
                <pre>{typeof content === 'object' ? JSON.stringify(content, null, 2) : content}</pre>
            </div>
        </div>
    );
}
