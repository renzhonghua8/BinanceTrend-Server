import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import app


class QualifiedLeaderTests(unittest.IsolatedAsyncioTestCase):
    async def test_only_qualified_leader_changes_trigger_notification(self) -> None:
        with tempfile.TemporaryDirectory() as directory, patch.object(
            app, "STATE_PATH", Path(directory) / "state.json"
        ):
            service = app.TrendService()
            marks = {
                "RAWUSDT": ("normal", 90.0),
                "GOODUSDT": ("qualified", 80.0),
                "NEXTUSDT": ("normal", 70.0),
            }
            notifications = []

            async def classification(symbol: str):
                return marks[symbol]

            async def notification(previous, current, qualified):
                notifications.append((previous, current, qualified))

            service.cached_classification = classification
            service.notify_leader_change = notification
            service.tickers = {
                "RAWUSDT": app.Ticker("RAWUSDT", 100.0, 30.0),
                "GOODUSDT": app.Ticker("GOODUSDT", 90.0, 20.0),
                "NEXTUSDT": app.Ticker("NEXTUSDT", 80.0, 10.0),
            }

            await service.evaluate_qualified_leader()
            self.assertEqual(service.leader, "GOODUSDT")
            self.assertEqual(notifications, [])

            # The raw #1 changes, while the highest-ranked qualified symbol does not.
            service.tickers["NEXTUSDT"].percent = 40.0
            await service.evaluate_qualified_leader()
            self.assertEqual(service.leader, "GOODUSDT")
            self.assertEqual(notifications, [])

            # A different symbol becomes the highest-ranked qualified symbol.
            marks["NEXTUSDT"] = ("qualified", 70.0)
            await service.evaluate_qualified_leader()
            self.assertEqual(service.leader, "NEXTUSDT")
            self.assertEqual(len(notifications), 1)
            self.assertEqual(notifications[0][0:2], ("GOODUSDT", "NEXTUSDT"))

            state = json.loads(app.STATE_PATH.read_text())
            self.assertEqual(state["qualifiedLeader"], "NEXTUSDT")

    async def test_no_qualified_symbol_does_not_send(self) -> None:
        with tempfile.TemporaryDirectory() as directory, patch.object(
            app, "STATE_PATH", Path(directory) / "state.json"
        ):
            service = app.TrendService()
            notifications = []

            async def classification(_: str):
                return "normal", 90.0

            async def notification(previous, current, qualified):
                notifications.append((previous, current, qualified))

            service.cached_classification = classification
            service.notify_leader_change = notification
            service.tickers = {
                "RAWUSDT": app.Ticker("RAWUSDT", 100.0, 30.0),
            }

            await service.evaluate_qualified_leader()
            await service.evaluate_qualified_leader()
            self.assertIsNone(service.leader)
            self.assertEqual(notifications, [])


if __name__ == "__main__":
    unittest.main()
