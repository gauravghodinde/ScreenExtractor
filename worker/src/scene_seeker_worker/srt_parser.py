"""
SRT Parser module for extracting subtitle entries with timestamps and text.
"""
import srt
from typing import List, Dict


class SubtitleEntry:
    """Represents a single subtitle entry with timestamp and text."""
    
    def __init__(self, index: int, start: str, end: str, text: str):
        self.index = index
        self.start = start
        self.end = end
        self.text = text
    
    def get_timestamp(self) -> str:
        """Returns the timestamp in SRT format."""
        return f"{self.start} --> {self.end}"
    
    def __repr__(self):
        return f"SubtitleEntry(index={self.index}, start={self.start}, text={self.text[:30]}...)"


class SRTParser:
    """Parser for SRT subtitle files."""
    
    def __init__(self, filepath: str):
        self.filepath = filepath
        self.entries: List[SubtitleEntry] = []
    
    def parse(self) -> List[SubtitleEntry]:
        """Parse the SRT file and return a list of subtitle entries."""
        with open(self.filepath, 'r', encoding='utf-8', errors='ignore') as f:
            srt_content = f.read()
        
        subs = srt.parse(srt_content)
        
        for sub in subs:
            entry = SubtitleEntry(
                index=sub.index,
                start=str(sub.start),
                end=str(sub.end),
                text=sub.content.replace('\n', ' ')
            )
            self.entries.append(entry)
        
        return self.entries
    
    def get_entries(self) -> List[SubtitleEntry]:
        """Get all parsed subtitle entries."""
        if not self.entries:
            self.parse()
        return self.entries
    
    def get_texts(self) -> List[str]:
        """Get all subtitle texts as a list."""
        if not self.entries:
            self.parse()
        return [entry.text for entry in self.entries]
