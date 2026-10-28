import asyncio
import json
import importlib
import random
from pathlib import Path
from datetime import datetime, timedelta

from astrbot.api.event import AstrMessageEvent, MessageChain, filter
from astrbot.api.provider import ProviderRequest
from astrbot.api.star import Context, Star, StarTools

from .engine import Discovery, target_configured


class WhaleDiscovery(Star):
    def __init__(self, context: Context, config):
        super().__init__(context)
        self.config = config
        self.engine = Discovery(StarTools.get_data_dir('astrbot_plugin_whale_discovery'), config,
                                self.generate, self.deliver, self.allowed, self.persona, self.logger, self.meme_options)
        self.task = None
        self.manual_lock = asyncio.Lock()

    async def initialize(self):
        self.task = asyncio.create_task(self.loop())

    async def terminate(self):
        if self.task:
            self.task.cancel()
            try:
                await self.task
            except asyncio.CancelledError:
                pass
        self.engine.db.close()

    def allowed(self, target):
        config = self.context.get_config(target)
        settings = config['platform_settings']
        # Opt-out by removing a recipient or its existing whitelist entry.
        return settings.get('enable_id_white_list', True) and target in settings.get('id_whitelist', [])

    async def persona(self, target):
        config = self.context.get_config(target)
        pid = config['agent_runner']['config']['persona']['persona_id']
        persona = await self.context.persona_manager.get_persona(pid)
        return persona.system_prompt

    async def generate(self, target, prompt, image, system):
        provider_id = await self.context.get_current_chat_provider_id(target)
        result = await asyncio.wait_for(self.context.llm_generate(
            chat_provider_id=provider_id, prompt=prompt, system_prompt=system,
            image_urls=[image] if image else [], request_max_retries=0,
        ), timeout=100)
        return result.completion_text

    async def deliver(self, item):
        chains = []
        if item['image']:
            chains.append(MessageChain().file_image(item['image']))
        chains.extend(MessageChain().message(line) for line in item['messages'])
        if self.engine.config.get('show_source', False):
            chains.append(MessageChain().message('来源：' + item['source'] + '\n' + item['url']))
        for index, chain in enumerate(chains):
            if index:
                await asyncio.sleep(random.uniform(0.8, 1.6))
            if not await self.context.send_message(item['target'], chain):
                return False
        if item.get('meme'):
            try:
                await asyncio.sleep(random.uniform(0.8, 1.6))
                await self.context.send_message(item['target'], MessageChain().file_image(item['meme']))
            except Exception as exc:
                self.logger.warning('Optional meme delivery failed (%s)', type(exc).__name__)
        return True

    async def meme_options(self, target):
        # Optional integration with meme_manager 5.x's pack resolver.
        star = self.context.get_registered_star('meme_manager')
        if not star or not star.activated or not star.star_cls:
            return {}
        try:
            base = star.star_cls.__class__.__module__.rsplit('.', 1)[0]
            resolver = importlib.import_module(base + '.backend.packs.resolver')
            categories = importlib.import_module(base + '.backend.packs.categories')
            pid = self.context.get_config(target)['agent_runner']['config']['persona']['persona_id']
            packs = resolver.resolve_pack_context(session_id=target, persona_id=pid)
            result = {}
            for category, description in packs['category_mapping'].items():
                if category in ('_review', 'needs_review'):
                    continue
                files = []
                for directory in packs['pack_dirs'].values():
                    root = (Path(directory) / 'memes').resolve()
                    folder = categories.resolve_safe_category_directory(root, category)
                    if folder.is_dir():
                        files.extend(p for p in folder.iterdir() if p.is_file()
                                     and p.suffix.lower() in ('.png', '.jpg', '.jpeg', '.gif', '.webp')
                                     and p.resolve().is_relative_to(root))
                if files:
                    result[category] = {'description': str(description), 'files': files}
            return result
        except Exception as exc:
            self.logger.info('Meme integration unavailable (%s)', type(exc).__name__)
            return {}

    async def loop(self):
        await asyncio.sleep(60)
        while True:
            try:
                await self.engine.tick()
            except Exception as exc:
                self.logger.warning('Discovery cycle skipped (%s)', type(exc).__name__)
            await asyncio.sleep(60)

    @filter.on_llm_request()
    async def remember_share(self, event: AstrMessageEvent, req: ProviderRequest):
        target = event.unified_msg_origin
        if (self.engine.config.get('enabled') and target_configured(target, self.engine.config, manual=True)
                and self.allowed(target)):
            req.system_prompt = (req.system_prompt or '') + (
                '\n你可以调用 discover_fresh_content 从订阅信源找真实的新鲜内容。'
                '当对方问最近有啥好玩的、想看新鲜事、找点好吃的或类似问题，且你判断需要外界内容时，自主决定调用。'
                '普通闲聊、回忆过去、用户明确不想看时不要调用。每轮最多一次。'
                '工具会直接发送图片和短句；成功后不要重复转述。没有结果或额度受限就如实说明，不能编造已搜索到的内容。'
            )
        # Only inject this recipient's recent confirmed shares into their reply context.
        rows = self.engine.db.execute("SELECT at,title,text FROM shares WHERE target=? AND status='sent' ORDER BY id DESC LIMIT 2",
                                      (event.unified_msg_origin,)).fetchall()
        recent = [r for r in reversed(rows) if self.engine.now() - datetime.fromisoformat(r['at']) < timedelta(days=1)]
        if recent:
            # Active sends bypass AstrBot's regular response-history writer. Restore
            # confirmed assistant turns here without rewriting the conversation DB.
            req.contexts = list(req.contexts or [])
            for row in recent:
                content = f"【此前我在 {row['at']} 主动分享：{row['title']}】\n{row['text']}"
                if not any(message.get('role') == 'assistant' and message.get('content') == content for message in req.contexts):
                    req.contexts.append({'role': 'assistant', 'content': content})
            req.system_prompt = (req.system_prompt or '') + (
                '\n对话历史中的主动分享是你已向当前聊天对象发出的内容。'
                '对方紧接着问哪个、这个、你最想吃哪个时，先结合最近分享的对象和选项理解，'
                '不要明明刚分享过选项却说对方没给。可以表达自己的偏好，但不要编造图片或文章里没有的信息。'
                '历史中的时间和主题标记用于理解上下文，不要复述这些标记。'
            )

    @filter.permission_type(filter.PermissionType.ADMIN)
    @filter.command('找点事情')
    async def discover_now(self, event: AstrMessageEvent):
        event.stop_event()
        target = event.unified_msg_origin
        if not target_configured(target, self.engine.config, manual=True) or not self.allowed(target):
            await event.send(event.plain_result('请先将当前私聊或群聊加入插件允许列表和AstrBot白名单，再由管理员发送 /找点事情。'))
            return
        if self.manual_lock.locked():
            await event.send(event.plain_result('正在找内容，等这次完成再试。'))
            return
        if not self.engine.can_send(self.engine.now(), manual=True):
            await event.send(event.plain_result('当前无法分享：请检查插件开关、静默时段或今天的分享额度。可发送 /新鲜事状态 查看。'))
            return
        async with self.manual_lock:
            await event.send(event.plain_result('我去翻翻有什么有意思的，找到就发给你。'))
            try:
                item = await self.engine.prepare(target)
                if item is None:
                    await event.send(event.plain_result('这次没有挑到合适的新内容，晚点再试吧。'))
                elif not await self.engine.dispatch(item, manual=True):
                    await event.send(event.plain_result('这次没能确认分享成功，可能触发了限制或发送异常。为避免重复，我不会自动重发。'))
            except Exception as exc:
                self.logger.warning('Manual discovery failed (%s)', type(exc).__name__)
                await event.send(event.plain_result('这次读取信源或生成分享失败了，稍后可以再试。'))

    @filter.llm_tool(name='discover_fresh_content')
    async def discover_fresh_content(self, event: AstrMessageEvent, topic: str = '') -> str:
        """从订阅信源发现真实的新鲜事，并直接给当前聊天对象发送图片、短句和可选表情包。

        当用户想知道最近有啥好玩的、新奇见闻或好吃的，且需要外部内容时调用。
        仅搜索已选RSS信源；并非通用网页搜索或私人记忆检索。每轮最多调用一次。
        成功后内容已发送，不要重复复述；没有结果时如实告知。

        Args:
            topic(string): 本次想找的内容，如新奇小游戏、诱人的甜点；泛泛找新鲜事时留空。
        """
        target = event.unified_msg_origin
        if not target_configured(target, self.engine.config, manual=True) or not self.allowed(target):
            return '当前会话不在允许的私聊或群聊列表中，未执行搜索或发送。'
        previous = event.get_extra('whale_discovery_tool_result')
        if previous:
            return previous
        if self.manual_lock.locked():
            return '正在处理一次找内容请求，请等待，不要重复调用。'
        if not self.engine.can_send(self.engine.now(), manual=True):
            result = '当前插件关闭、处于静默时段或当日分享额度已用完；没有执行搜索，请如实说明。'
            event.set_extra('whale_discovery_tool_result', result)
            return result
        event.set_extra('whale_discovery_tool_result', '本轮已经请求找内容，请勿重复调用。')
        async with self.manual_lock:
            try:
                item = await self.engine.prepare(target, topic=topic.strip()[:300])
                if item is None:
                    result = '读取订阅信源后没有挑到符合本次主题的新内容，没有发送分享；请自然说明，不要编造。'
                elif await self.engine.dispatch(item, manual=True):
                    result = '已向当前聊天对象发送分享的图片、短句和可选表情包。本轮任务完成，不要重复发送或复述。'
                    event.stop_event()
                else:
                    result = '未能确认分享成功，可能触发限制或发送异常。不要声称发送成功，也不要自动重试。'
            except Exception as exc:
                self.logger.warning('Discovery tool failed (%s)', type(exc).__name__)
                result = '读取信源或生成内容时出错，未完成分享。请如实说明，不要编造搜索结果。'
            event.set_extra('whale_discovery_tool_result', result)
            return result

    @filter.permission_type(filter.PermissionType.ADMIN)
    @filter.command('新鲜事状态')
    async def status_command(self, event: AstrMessageEvent):
        yield event.plain_result(json.dumps(self.engine.status(), ensure_ascii=False, indent=2))
