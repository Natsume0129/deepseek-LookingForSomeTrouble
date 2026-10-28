"""Command pipeline regression tests; run in an AstrBot development environment."""

import asyncio
import importlib
import logging
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, Mock

from astrbot.api.event import MessageEventResult
from astrbot.core.pipeline.context_utils import call_handler

package = types.ModuleType('discovery_command_test')
package.__path__ = [str(Path(__file__).parent)]
sys.modules[package.__name__] = package
WhaleDiscovery = importlib.import_module('discovery_command_test.main').WhaleDiscovery


class ManualCommand(unittest.IsolatedAsyncioTestCase):
    async def run_command(self, item):
        plugin = object.__new__(WhaleDiscovery)
        target = 'demo:FriendMessage:10001'
        plugin.engine = types.SimpleNamespace(
            config={'recipients': [target]}, now=Mock(), can_send=Mock(return_value=True),
            prepare=AsyncMock(return_value=item), dispatch=AsyncMock(return_value=True),
        )
        plugin.allowed = Mock(return_value=True)
        plugin.manual_lock = asyncio.Lock()
        plugin.logger = logging.getLogger('command-test')
        stopped = False

        def stop():
            nonlocal stopped
            stopped = True

        event = types.SimpleNamespace(
            unified_msg_origin=target, stop_event=stop, send=AsyncMock(),
            plain_result=lambda text: MessageEventResult().message(text), set_result=Mock(),
        )
        # The real pipeline stops iterating after a yielded result marks the event stopped.
        async for _ in call_handler(event, plugin.discover_now):
            if stopped:
                break
        return plugin, event

    async def test_acknowledgement_does_not_end_search(self):
        item = {'title': 'Test article'}
        plugin, event = await self.run_command(item)
        event.send.assert_awaited_once()
        plugin.engine.prepare.assert_awaited_once_with(event.unified_msg_origin)
        plugin.engine.dispatch.assert_awaited_once_with(item, manual=True)

    async def test_no_match_has_terminal_reply(self):
        plugin, event = await self.run_command(None)
        self.assertEqual(event.send.await_count, 2)
        plugin.engine.dispatch.assert_not_awaited()


if __name__ == '__main__':
    unittest.main()
