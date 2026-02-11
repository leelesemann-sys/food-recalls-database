import json
from pathlib import Path
from typing import List
import pandas as pd

from utils.logger import setup_logger

logger = setup_logger()


class StateManager:
    """Manages notification state to prevent duplicate alerts."""

    def __init__(self, state_file: str = None):
        """
        Initialize StateManager.

        Args:
            state_file: Path to state file. Defaults to state/notified_recalls.json
        """
        if state_file is None:
            self.state_file = Path(__file__).parent.parent.parent / "state" / "notified_recalls.json"
        else:
            self.state_file = Path(state_file)

        self.state_file.parent.mkdir(exist_ok=True)
        self._notified_recalls: set = self._load_state()

    def _load_state(self) -> set:
        """Load notified recall numbers from state file."""
        if not self.state_file.exists():
            logger.info(f"State file not found, creating new: {self.state_file}")
            return set()

        try:
            with open(self.state_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                return set(data.get('notified_recalls', []))
        except json.JSONDecodeError as e:
            logger.warning(f"State file corrupt, reinitializing: {e}")
            return set()
        except Exception as e:
            logger.error(f"Error loading state file: {e}")
            return set()

    def _save_state(self) -> bool:
        """Save notified recall numbers to state file."""
        try:
            with open(self.state_file, 'w', encoding='utf-8') as f:
                json.dump({
                    'notified_recalls': list(self._notified_recalls)
                }, f, indent=2)
            return True
        except Exception as e:
            logger.error(f"Error saving state file: {e}")
            return False

    def filter_new_class1_recalls(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Filter DataFrame for new Class I recalls that haven't been notified yet.

        Args:
            df: DataFrame with recall data

        Returns:
            DataFrame containing only new Class I recalls
        """
        if df.empty:
            return df

        # Filter for Class I recalls
        class1_mask = df['classification'].str.upper() == 'CLASS I'
        class1_df = df[class1_mask]

        if class1_df.empty:
            logger.debug("No Class I recalls in dataset")
            return class1_df

        # Filter out already notified recalls
        new_mask = ~class1_df['recall_number'].isin(self._notified_recalls)
        new_class1 = class1_df[new_mask].copy()

        logger.debug(f"Found {len(class1_df)} Class I recalls, {len(new_class1)} are new")
        return new_class1

    def mark_as_notified(self, recall_numbers: List[str]) -> bool:
        """
        Mark recall numbers as notified.

        Args:
            recall_numbers: List of recall numbers to mark as notified

        Returns:
            True if state was saved successfully
        """
        self._notified_recalls.update(recall_numbers)
        success = self._save_state()

        if success:
            logger.info(f"Marked {len(recall_numbers)} recalls as notified")
        return success

    def is_notified(self, recall_number: str) -> bool:
        """Check if a recall number has been notified."""
        return recall_number in self._notified_recalls

    def get_notified_count(self) -> int:
        """Get count of notified recalls."""
        return len(self._notified_recalls)
