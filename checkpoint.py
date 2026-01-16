import json
import os
import logging

logger = logging.getLogger("HeavyAggregator")

class CheckpointManager:
    def __init__(self, filename="checkpoint.json"):
        self.filename = filename
        self.state = self.load()

    def load(self):
        if not os.path.exists(self.filename):
            return {}
        
        try:
            with open(self.filename, 'r') as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError) as e:
            logger.warning(f"Failed to load checkpoint file: {e}")
            return {}

    def save(self, key, value):
        """Updates a specific key in the state and saves to disk."""
        self.state[key] = value
        self._write_to_disk()

    def update(self, new_state):
        """Updates multiple keys and saves."""
        self.state.update(new_state)
        self._write_to_disk()

    def _write_to_disk(self):
        try:
            with open(self.filename, 'w') as f:
                json.dump(self.state, f, indent=2)
        except OSError as e:
            logger.error(f"Failed to save checkpoint: {e}")

    def clear(self):
        if os.path.exists(self.filename):
            try:
                os.remove(self.filename)
                self.state = {}
                logger.info("Checkpoint cleared.")
            except OSError as e:
                logger.error(f"Failed to clear checkpoint: {e}")

    def get(self, key, default=None):
        return self.state.get(key, default)
