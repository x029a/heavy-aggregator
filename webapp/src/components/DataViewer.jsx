import React, { useState, useEffect } from 'react';
import { Prism as SyntaxHighlighter } from 'react-syntax-highlighter';
import { vscDarkPlus } from 'react-syntax-highlighter/dist/esm/styles/prism';

export default function DataViewer({ filePath }) {
    const [content, setContent] = useState(null);
    const [loading, setLoading] = useState(false);
    // Removed error state as per instruction

    useEffect(() => {
        if (!filePath) {
            setContent(null); // Clear content when no file is selected
            return;
        }

        setLoading(true);
        // Updated fetch URL to encodeURIComponent and simplified error handling
        fetch(`/api/files/content?path=${encodeURIComponent(filePath)}`)
            .then(res => {
                if (!res.ok) throw new Error("Failed to load file");
                return res.text();
            })
            .then(text => {
                // Simplified content setting, removed JSON.parse try/catch as SyntaxHighlighter will handle it
                setContent(text);
                // Removed error state clearing
            })
            .catch(err => {
                console.error(err); // Log error
                setContent("Error loading file."); // Display generic error message
                // Removed error state setting
            })
            .finally(() => setLoading(false)); // Ensure loading is always set to false
    }, [filePath]);

    if (!filePath) return <div className="data-viewer-empty">Select a file to view content</div>; // Updated class name
    if (loading) return <div className="data-viewer-loading">Loading...</div>; // Updated class name
    // Removed if (error) block as per instruction

    return (
        <div className="data-viewer">
            <div className="file-header">{filePath}</div> {/* Changed h3 to div with class */}
            <div className="file-content-wrapper"> {/* New wrapper div */}
                <SyntaxHighlighter
                    language="json" // Assuming content is JSON or should be highlighted as such
                    style={vscDarkPlus}
                    customStyle={{ margin: 0, padding: '1rem', background: 'transparent' }}
                >
                    {content || ''} {/* Display content, or empty string if null */}
                </SyntaxHighlighter>
            </div>
        </div>
    );
}
