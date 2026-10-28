"""Command pipeline regression tests; run in an AstrBot development environment."""

import asyncio
import importlib
import logging
import sqlite3
import sys
import types
import unittest
from pathlib import Path
from datetime import datetime
from unittest.mock import AsyncMock, Mock, patch

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


class ShareDelivery(unittest.IsolatedAsyncioTestCase):
    async def test_image_then_separate_lines_then_optional_meme(self):
        plugin = object.__new__(WhaleDiscovery)
        plugin.context = types.SimpleNamespace(send_message=AsyncMock(return_value=True))
        plugin.engine = types.SimpleNamespace(config={})
        plugin.logger = logging.getLogger('delivery-test')
        item = dict(target='demo:FriendMessage:10001', image='article.jpg',
                    messages=['欸你看', '好蓝啊', '有点神'], source='NASA',
                    url='https://www.nasa.gov/example', meme='happy.gif')
        with patch('discovery_command_test.main.asyncio.sleep', new_callable=AsyncMock):
            self.assertTrue(await plugin.deliver(item))
        chains = [call.args[1].chain for call in plugin.context.send_message.await_args_list]
        self.assertEqual([type(chain[0]).__name__ for chain in chains], ['Image','Plain','Plain','Plain','Image'])
        self.assertEqual([chains[i][0].text for i in (1,2,3)],item['messages'])
        self.assertFalse(any('https://' in getattr(c[0],'text','') for c in chains))

    async def test_source_switch_and_no_meme_choice(self):
        plugin = object.__new__(WhaleDiscovery)
        plugin.context = types.SimpleNamespace(send_message=AsyncMock(return_value=True))
        plugin.engine = types.SimpleNamespace(config={'show_source':True})
        item = dict(target='demo:FriendMessage:10001', image=None, messages=['欸你看','有点神'],
                    source='NASA',url='https://www.nasa.gov/example',meme=None)
        with patch('discovery_command_test.main.asyncio.sleep', new_callable=AsyncMock):
            self.assertTrue(await plugin.deliver(item))
        chains=[call.args[1].chain for call in plugin.context.send_message.await_args_list]
        self.assertEqual(len(chains),3)
        self.assertEqual(chains[-1][0].text,'来源：NASA\nhttps://www.nasa.gov/example')


class DiscoveryTool(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.plugin = object.__new__(WhaleDiscovery)
        target = 'demo:FriendMessage:10001'
        self.plugin.engine = types.SimpleNamespace(
            config={'recipients':[target]}, now=Mock(), can_send=Mock(return_value=True),
            prepare=AsyncMock(return_value={'title':'Test'}), dispatch=AsyncMock(return_value=True),
        )
        self.plugin.allowed = Mock(return_value=True)
        self.plugin.manual_lock = asyncio.Lock()
        self.plugin.logger = logging.getLogger('tool-test')
        extras = {}
        self.event = types.SimpleNamespace(unified_msg_origin=target, get_extra=extras.get,
                                           set_extra=extras.__setitem__, stop_event=Mock())

    async def test_tool_sends_once_and_passes_topic(self):
        first = await self.plugin.discover_fresh_content(self.event, '新奇小游戏')
        second = await self.plugin.discover_fresh_content(self.event, '甜品')
        self.assertEqual(first,second)
        self.plugin.engine.prepare.assert_awaited_once_with(self.event.unified_msg_origin,topic='新奇小游戏')
        self.plugin.engine.dispatch.assert_awaited_once_with({'title':'Test'},manual=True)
        self.event.stop_event.assert_called_once()

    async def test_tool_respects_existing_limits(self):
        self.plugin.engine.can_send.return_value=False
        result=await self.plugin.discover_fresh_content(self.event)
        self.assertIn('额度',result)
        self.plugin.engine.prepare.assert_not_awaited()

        self.event.stop_event.assert_not_called()

    async def test_tool_reports_no_results_without_sending(self):
        self.plugin.engine.prepare.return_value=None
        result=await self.plugin.discover_fresh_content(self.event,'甜品')
        self.assertIn('没有挑到',result)
        self.plugin.engine.dispatch.assert_not_awaited()
        self.event.stop_event.assert_not_called()

    async def test_tool_does_not_send_to_unlisted_session(self):
        self.event.unified_msg_origin='demo:GroupMessage:20001'
        await self.plugin.discover_fresh_content(self.event)
        self.plugin.engine.prepare.assert_not_awaited()


class SharedContext(unittest.IsolatedAsyncioTestCase):
    async def test_followup_contains_own_confirmed_share_without_duplicates(self):
        db=sqlite3.connect(':memory:');db.row_factory=sqlite3.Row
        self.addCleanup(db.close)
        db.execute('CREATE TABLE shares(id INTEGER PRIMARY KEY,at TEXT,target TEXT,title TEXT,text TEXT,status TEXT)')
        db.executemany('INSERT INTO shares VALUES(?,?,?,?,?,?)',[
            (1,'2026-10-28T12:00:00+09:00','demo:FriendMessage:10001','泡面','蓝色是鸡盐味，褐色是豚骨','sent'),
            (2,'2026-10-28T12:01:00+09:00','demo:FriendMessage:10002','另一个人的话题','私人内容','sent'),
            (3,'2026-10-28T12:02:00+09:00','demo:FriendMessage:10001','没确认发出的','不该出现','uncertain'),
        ])
        plugin=object.__new__(WhaleDiscovery)
        plugin.engine=types.SimpleNamespace(db=db,config={'enabled':False},now=lambda:datetime.fromisoformat('2026-10-28T12:03:00+09:00'))
        event=types.SimpleNamespace(unified_msg_origin='demo:FriendMessage:10001')
        req=types.SimpleNamespace(system_prompt='既有人设',contexts=[{'role':'user','content':'之前的聊天'}])
        await plugin.remember_share(event,req)
        self.assertEqual(req.contexts[-1]['role'],'assistant')
        self.assertIn('鸡盐味',req.contexts[-1]['content'])
        self.assertNotIn('私人内容',str(req.contexts))
        self.assertNotIn('不该出现',str(req.contexts))
        await plugin.remember_share(event,req)
        self.assertEqual(len(req.contexts),2)


if __name__ == '__main__':
    unittest.main()
