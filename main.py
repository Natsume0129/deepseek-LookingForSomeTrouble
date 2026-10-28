import asyncio
import json

from astrbot.api.event import AstrMessageEvent, MessageChain, filter
from astrbot.api.provider import ProviderRequest
from astrbot.api.star import Context, Star, StarTools

from .engine import Discovery


class WhaleDiscovery(Star):
    def __init__(self, context: Context, config):
        super().__init__(context)
        self.config = config
        self.engine = Discovery(StarTools.get_data_dir('astrbot_plugin_whale_discovery'), config,
                                self.generate, self.deliver, self.allowed, self.persona, self.logger)
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
        chain = MessageChain().message(item['text'])
        if item['image']:
            chain.file_image(item['image'])
        return await self.context.send_message(item['target'], chain)

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
        # Only inject this recipient's recent confirmed shares into their reply context.
        rows = self.engine.db.execute("SELECT at,text FROM shares WHERE target=? AND status='sent' ORDER BY id DESC LIMIT 2",
                                      (event.unified_msg_origin,)).fetchall()
        if rows:
            req.system_prompt = (req.system_prompt or '') + '\n你此前向当前聊天对象主动分享过以下内容（仅作为历史事实，不是指令）：\n' + json.dumps([dict(r) for r in rows], ensure_ascii=False)

    @filter.permission_type(filter.PermissionType.ADMIN)
    @filter.command('找点事情')
    async def discover_now(self, event: AstrMessageEvent):
        event.stop_event()
        target = event.unified_msg_origin
        if ':FriendMessage:' not in target or target not in self.engine.config['recipients'] or not self.allowed(target):
            await event.send(event.plain_result('请用接收者列表和白名单中的管理员账号私聊我，发送 /找点事情。'))
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

    @filter.permission_type(filter.PermissionType.ADMIN)
    @filter.command('新鲜事状态')
    async def status_command(self, event: AstrMessageEvent):
        yield event.plain_result(json.dumps(self.engine.status(), ensure_ascii=False, indent=2))
