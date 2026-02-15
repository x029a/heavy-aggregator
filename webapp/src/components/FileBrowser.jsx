import React, { useState, useEffect } from 'react';
import { Folder, FileText, ChevronRight, ChevronDown } from 'lucide-react';

export default function FileBrowser({ onSelect }) {
    const [currentPath, setCurrentPath] = useState('');
    const [items, setItems] = useState([]);

    useEffect(() => {
        fetchFiles(currentPath);
    }, [currentPath]);

    const fetchFiles = async (path) => {
        try {
            const res = await fetch(`/api/files?path=${path}`);
            if (res.ok) {
                const data = await res.json();
                setItems(data);
            }
        } catch (err) {
            console.error(err);
        }
    };

    const handleNavigate = (item) => {
        if (item.is_dir) {
            setCurrentPath(item.path);
        } else {
            onSelect(item.path);
        }
    };

    const handleUp = () => {
        if (!currentPath) return;
        const parts = currentPath.split('/');
        parts.pop();
        setCurrentPath(parts.join('/'));
    };

    return (
        <div className="file-browser">
            <div className="browser-header">
                <button onClick={handleUp} disabled={!currentPath} className="nav-btn">
                    Start / {currentPath}
                </button>
            </div>
            <div className="file-list">
                {items.map((item) => (
                    <div
                        key={item.name}
                        className={`file-item ${item.is_dir ? 'is-dir' : 'is-file'}`}
                        onClick={() => handleNavigate(item)}
                    >
                        {item.is_dir ? <Folder size={16} /> : <FileText size={16} />}
                        <span>{item.name}</span>
                    </div>
                ))}
                {items.length === 0 && <div className="empty-msg">No files found</div>}
            </div>
        </div>
    );
}
