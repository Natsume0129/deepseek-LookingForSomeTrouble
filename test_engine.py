import asyncio
import logging
import tempfile
import unittest
from unittest.mock import AsyncMock, patch
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from engine import Discovery, quiet, in_window


class SharingLimits(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.config = dict(enabled=True, timezone='Asia/Tokyo', quiet_start='03:00', quiet_end='09:30',
                           daily_min=3, daily_max=3, sharing_windows=['09:30-23:59'], recipients=['demo_bot:FriendMessage:10001'])
        self.clock = datetime(2026, 10, 28, 10, 0, tzinfo=ZoneInfo('Asia/Tokyo'))
        self.sent = []
        async def deliver(item):
            self.sent.append(item)
            return True
        self.engine = Discovery(self.temp.name, self.config, None, deliver, lambda _: True, None, logging.getLogger('test'))
        self.engine.now = lambda: self.clock

    def tearDown(self):
        self.engine.db.close()
        self.temp.cleanup()

    def item(self, index=1):
        return dict(target=self.config['recipients'][0], url=f'https://www.nasa.gov/{index}', title=f'Article {index}', text='Text', image=None)

    def test_quiet_boundaries(self):
        for hour, mins, expected in [(2,59,False),(3,0,True),(9,29,True),(9,30,False)]:
            self.assertEqual(quiet(self.clock.replace(hour=hour,minute=mins),self.config),expected)

    async def test_total_cap_survives_reload_and_concurrency(self):
        for index in range(3):
            self.assertTrue(await self.engine.dispatch(self.item(index)))
            self.clock += timedelta(hours=4)
        self.assertFalse(await self.engine.dispatch(self.item(99)))
        self.engine.db.close()
        self.engine = Discovery(self.temp.name,self.config,None,self.engine.deliver,lambda _:True,None,logging.getLogger('test'))
        self.engine.now=lambda:self.clock
        self.assertFalse(await self.engine.dispatch(self.item(100)))
        self.assertEqual(len(self.sent),3)

    async def test_concurrent_duplicate(self):
        result=await asyncio.gather(self.engine.dispatch(self.item()),self.engine.dispatch(self.item()))
        self.assertEqual(result.count(True),1)
        self.clock += timedelta(hours=4)
        self.assertFalse(await self.engine.dispatch(self.item()))

    async def test_quiet_rechecked_after_preparation(self):
        item=self.item()
        self.clock=self.clock.replace(hour=3)
        self.assertFalse(await self.engine.dispatch(item))
        self.assertEqual(self.sent,[])

    async def test_recipient_and_whitelist(self):
        item=self.item();item['target']='demo_bot:GroupMessage:20001'
        self.assertFalse(await self.engine.dispatch(item))
        self.engine.allowed=lambda _:False
        self.assertFalse(await self.engine.dispatch(self.item()))

    async def test_uncertain_send_is_not_retried(self):
        async def failing(item):
            raise TimeoutError('Unknown delivery outcome')
        self.engine.deliver=failing
        self.assertFalse(await self.engine.dispatch(self.item()))
        self.assertEqual(self.engine.status()['used_today'],1)
        self.clock += timedelta(hours=4)
        self.assertFalse(await self.engine.dispatch(self.item()))

    async def test_missed_slots_do_not_burst(self):
        self.clock=self.clock.replace(hour=23,minute=0)
        with self.engine.db:
            self.engine.db.execute('INSERT INTO plans VALUES(?,?)', ('2026-10-28',1))
            self.engine.db.execute('INSERT INTO slots(day,slot,due) VALUES(?,?,?)', ('2026-10-28',0,self.clock.replace(hour=10).isoformat()))
        await self.engine.tick()
        self.assertEqual(self.sent,[])
        self.assertTrue(all(r['state']=='used' for r in self.engine.status()['schedule']))

    def test_configurable_windows_random_count_and_reload(self):
        self.engine.config.update(daily_min=2,daily_max=4,sharing_windows=['10:00-12:00','16:00-19:00'],min_interval_minutes=60)
        with patch('engine.random.randint',return_value=3):
            self.engine.schedule(self.clock)
        first=self.engine.status()['schedule']
        self.assertEqual(len(first),3)
        times=[datetime.fromisoformat(r['due']) for r in first]
        self.assertTrue(all(any(in_window(t.hour*60+t.minute,w) for w in self.engine.config['sharing_windows']) for t in times))
        self.assertTrue(all(b-a>=timedelta(minutes=60) for a,b in zip(times,times[1:])))
        with patch('engine.random.randint',side_effect=AssertionError('Schedule must persist')):
            self.engine.schedule(self.clock)
        self.assertEqual(first,self.engine.status()['schedule'])

    def test_overnight_and_zero_day(self):
        self.assertTrue(in_window(23*60,'22:00-02:00'))
        self.assertTrue(in_window(60,'22:00-02:00'))
        self.assertFalse(in_window(120,'22:00-02:00'))
        self.engine.config.update(daily_min=0,daily_max=0)
        self.engine.schedule(self.clock)
        self.assertEqual(self.engine.status()['schedule'],[])
        with patch('engine.random.randint',side_effect=AssertionError('Zero day must persist')):
            self.engine.schedule(self.clock)

    async def test_window_rechecked_before_send(self):
        self.engine.config['sharing_windows']=['10:00-11:00']
        self.clock=self.clock.replace(hour=11)
        self.assertFalse(await self.engine.dispatch(self.item()))

    async def test_manual_skips_schedule_but_respects_cap(self):
        self.engine.config['sharing_windows']=['18:00-19:00']
        self.assertFalse(self.engine.can_send(self.clock))
        for index in range(3):
            self.assertTrue(await self.engine.dispatch(self.item(index), manual=True))
        self.assertFalse(await self.engine.dispatch(self.item(99), manual=True))
        self.assertEqual(len(self.sent),3)

    async def test_manual_respects_quiet_disabled_and_recipient(self):
        self.clock=self.clock.replace(hour=3)
        self.assertFalse(await self.engine.dispatch(self.item(),manual=True))
        self.clock=self.clock.replace(hour=10)
        self.engine.config['enabled']=False
        self.assertFalse(await self.engine.dispatch(self.item(),manual=True))
        self.engine.config['enabled']=True
        self.engine.allowed=lambda _:False
        self.assertFalse(await self.engine.dispatch(self.item(),manual=True))
        self.assertEqual(self.sent,[])

    async def test_only_selected_sources_are_fetched(self):
        self.engine.config['sources']=['nasa']
        with patch('engine.fetch',new_callable=AsyncMock,return_value=(b'<rss><channel/></rss>','https://www.nasa.gov/feed/')) as mocked:
            self.assertEqual(await self.engine.candidates(),[])
            mocked.assert_awaited_once_with('https://www.nasa.gov/feed/')
        self.engine.config['sources']=[]
        with patch('engine.fetch',new_callable=AsyncMock) as mocked:
            self.assertEqual(await self.engine.candidates(),[])
            mocked.assert_not_awaited()

    async def test_interest_configuration_reaches_selector(self):
        self.engine.config.update(interests='烘焙和猫咪',avoid_topics='手机发布会')
        self.engine.candidates=AsyncMock(return_value=[{'title':'example'}])
        self.engine.generate=AsyncMock(return_value='{"indices":[]}')
        self.assertIsNone(await self.engine.prepare(self.config['recipients'][0]))
        prompt=self.engine.generate.call_args.args[1]
        self.assertIn('烘焙和猫咪',prompt)
        self.assertIn('手机发布会',prompt)

    async def test_model_selects_optional_meme_and_short_messages(self):
        article=dict(title='Test sky',url='https://www.nasa.gov/example',category='太空自然',source='NASA')
        self.engine.candidates=AsyncMock(return_value=[article])
        self.engine.persona=AsyncMock(return_value='随意聊天')
        self.engine.memes=AsyncMock(return_value={'happy':{'description':'开心','files':['happy.gif']}})
        self.engine.generate=AsyncMock(side_effect=['{"indices":[0]}','{"share":true,"interesting":9,"messages":["欸你看","好蓝啊","有点神"],"meme_category":"happy"}'])
        html=('<article>'+('Article facts. '*25)+'</article>').encode()
        with patch('engine.fetch',new_callable=AsyncMock,return_value=(html,article['url'])):
            result=await self.engine.prepare(self.config['recipients'][0])
        self.assertEqual(result['messages'],['欸你看','好蓝啊','有点神'])
        self.assertEqual(result['meme'],'happy.gif')
        self.assertNotIn('https://',result['text'])
        self.assertEqual(result['url'],article['url'])


if __name__=='__main__':
    unittest.main()
