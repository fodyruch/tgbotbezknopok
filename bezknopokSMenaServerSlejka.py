import asyncio
import logging
import json
import os
from datetime import datetime, timedelta
from typing import Dict, Optional, Any, Tuple
import aiohttp
from pytz import timezone
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes
from functools import wraps
from dotenv import load_dotenv

# Загрузка переменных окружения из .env файла
load_dotenv()

# Настройка логов
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Конфигурация (загружаем из переменных окружения)
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
API_KEY = os.getenv("BATTLEMETRICS_API_KEY")
TIMEZONE = timezone('Europe/Moscow')
DATA_FILE = 'tracked_players.json'
MAX_PLAYERS_PER_USER = 10

# Проверка наличия необходимых переменных окружения
if not TELEGRAM_TOKEN or not API_KEY:
    raise ValueError("Необходимо установить TELEGRAM_TOKEN и BATTLEMETRICS_API_KEY в .env файле")

# Глобальные переменные
user_sessions = {}  # {chat_id: {'tracked_players': {player_id: server_id}, 'status_messages': {player_id: message_id}}}
active_tasks = {}  # Хранение активных задач мониторинга

# Rate limiting
command_cooldowns = {}  # {chat_id: last_time}


def rate_limit(seconds=5):
    def decorator(func):
        @wraps(func)
        async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
            chat_id = update.effective_chat.id
            now = datetime.now()
            if chat_id in command_cooldowns:
                last_time = command_cooldowns[chat_id]
                if (now - last_time).total_seconds() < seconds:
                    await update.message.reply_text("⏳ Не так быстро! Подождите немного.")
                    return
            command_cooldowns[chat_id] = now
            return await func(update, context, *args, **kwargs)
        return wrapper
    return decorator


class BattleMetricsAPI:
    def __init__(self, api_key: str, session: aiohttp.ClientSession):
        self.api_key = api_key
        self.session = session
        self.base_url = "https://api.battlemetrics.com"  # ИСПРАВЛЕНО: Убраны лишние пробелы
        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Accept": "application/json"
        }

    async def make_api_request(self, url: str) -> Dict[str, Any]:
        """Выполняет API запрос с обработкой ошибок и повторными попытками"""
        try:
            async with self.session.get(url, headers=self.headers, timeout=aiohttp.ClientTimeout(total=10)) as response:
                if response.status == 200:
                    return await response.json()
                elif response.status == 429:  # Rate limit
                    logger.warning("Rate limit hit, waiting...")
                    await asyncio.sleep(60)
                    return await self.make_api_request(url)
                else:
                    logger.warning(f"API error {response.status}: {url}")
                    return {"errors": [{"status": response.status}]}
        except asyncio.TimeoutError:
            logger.error(f"Timeout for {url}")
            return {"errors": [{"detail": "Request timeout"}]}
        except Exception as e:
            logger.error(f"Error making API request to {url}: {e}")
            return {"errors": [{"detail": str(e)}]}

    async def get_player_info(self, player_id: str) -> Dict[str, Any]:
        url = f"{self.base_url}/players/{player_id}"
        return await self.make_api_request(url)

    async def get_server_info(self, server_id: str) -> Dict[str, Any]:
        url = f"{self.base_url}/servers/{server_id}"
        return await self.make_api_request(url)

    async def get_player_sessions(self, player_id: str) -> Dict[str, Any]:
        url = f"{self.base_url}/players/{player_id}/relationships/sessions"
        return await self.make_api_request(url)

    async def is_player_online(self, player_id: str, server_id: str) -> bool:
        sessions = await self.get_player_sessions(player_id)
        if not sessions.get("data"):
            return False
        for session in sessions["data"]:
            server_data = session["relationships"].get("server", {}).get("data")
            if server_data and server_data.get("id") == str(server_id):
                return session["attributes"].get("stop") is None
        return False

    async def get_current_session_time(self, player_id: str, server_id: str) -> timedelta:
        sessions = await self.get_player_sessions(player_id)
        if not sessions.get("data"):
            return timedelta(0)
        for session in sessions["data"]:
            server_data = session["relationships"].get("server", {}).get("data")
            if not server_data or server_data.get("id") != str(server_id):
                continue
            attributes = session["attributes"]
            start_str = attributes.get("start")
            stop_str = attributes.get("stop")
            try:
                if start_str:
                    # Убираем миллисекунды если есть
                    if '.' in start_str:
                        start_str = start_str.split('.')[0] + 'Z'
                    start = datetime.strptime(start_str, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone('UTC'))
                    if stop_str:
                        if '.' in stop_str:
                            stop_str = stop_str.split('.')[0] + 'Z'
                        stop = datetime.strptime(stop_str, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone('UTC'))
                    else:
                        stop = datetime.utcnow().replace(tzinfo=timezone('UTC'))
                    return stop - start
            except Exception as e:
                logger.error(f"Error parsing session time: {e}")
                continue
        return timedelta(0)

    # НОВАЯ ФУНКЦИЯ: Получить текущий сервер игрока
    async def get_current_server_for_player(self, player_id: str) -> Tuple[Optional[str], bool]:
        """
        Получает текущий сервер игрока
        Возвращает: (server_id, is_online) или (None, False) если игрок не найден
        """
        sessions = await self.get_player_sessions(player_id)
        if not sessions.get("data"):
            return None, False
            
        # Ищем активную сессию (где stop=null)
        for session in sessions["data"]:
            server_data = session["relationships"].get("server", {}).get("data")
            if server_data and "id" in server_data:
                stop_time = session["attributes"].get("stop")
                if stop_time is None:  # Активная сессия
                    return server_data["id"], True
                    
        # Если нет активной сессии, ищем последнюю по времени
        latest_session = None
        latest_time = None
        
        for session in sessions["data"]:
            server_data = session["relationships"].get("server", {}).get("data")
            if server_data and "id" in server_data:
                start_time_str = session["attributes"].get("start")
                if start_time_str:
                    try:
                        # Парсим время начала сессии
                        if '.' in start_time_str:
                            start_time_str = start_time_str.split('.')[0] + 'Z'
                        session_start = datetime.strptime(start_time_str, "%Y-%m-%dT%H:%M:%SZ")
                        
                        # Если это самая последняя сессия по времени
                        if latest_time is None or session_start > latest_time:
                            latest_time = session_start
                            latest_session = session
                    except Exception as e:
                        logger.error(f"Error parsing session time: {e}")
                        continue
        
        if latest_session:
            server_data = latest_session["relationships"].get("server", {}).get("data")
            if server_data and "id" in server_data:
                return server_data["id"], False  # Игрок оффлайн, но возвращаем последний сервер
                
        return None, False


async def format_duration(td: timedelta) -> str:
    seconds = int(td.total_seconds())
    hours, remainder = divmod(seconds, 3600)
    minutes, _ = divmod(remainder, 60)
    return f"{hours}ч {minutes}м"


async def get_player_status_message(player_id: str, server_id: str, api: BattleMetricsAPI) -> str:
    player_info_task = asyncio.create_task(api.get_player_info(player_id))
    server_info_task = asyncio.create_task(api.get_server_info(server_id))
    
    player_info = await player_info_task
    server_info = await server_info_task

    if 'errors' in player_info:
        player_name = f"ID:{player_id}"
    else:
        player_name = player_info.get("data", {}).get("attributes", {}).get("name", f"ID:{player_id}")

    if 'errors' in server_info:
        server_name = f"Сервер {server_id}"
    else:
        server_name = server_info.get("data", {}).get("attributes", {}).get("name", f"Сервер {server_id}")

    is_online_task = asyncio.create_task(api.is_player_online(player_id, server_id))
    session_time_task = asyncio.create_task(api.get_current_session_time(player_id, server_id))
    
    is_online = await is_online_task
    session_time = await session_time_task
    
    formatted_time = await format_duration(session_time)
    status = "🟢ОНЛАЙН🟢" if is_online else "🔴ОФФЛАЙН🔴"
    current_time = datetime.now(TIMEZONE).strftime("%d.%m, %H:%M:%S")

    return (
        f"✅ Добавлен игрок:\n"
        f"👤 {player_name}\n"
        f"🆔 ID: {player_id}\n"
        f"🌐 Сервер: {server_name} ({server_id})\n"
        f"🔄 Статус: {status}\n"
        f"⏱ Время на сервере: {formatted_time}\n"
        f"🕒 {current_time}"
    )


async def update_status_message(chat_id: int, player_id: str, server_id: str, context: ContextTypes.DEFAULT_TYPE, message_text: str = None):
    # Создаем временную сессию для этого запроса
    async with aiohttp.ClientSession() as session:
        api = BattleMetricsAPI(API_KEY, session)
        if message_text is None:
            message_text = await get_player_status_message(player_id, server_id, api)
        if chat_id in user_sessions and 'status_messages' in user_sessions[chat_id]:
            if player_id in user_sessions[chat_id]['status_messages']:
                try:
                    await context.bot.edit_message_text(
                        chat_id=chat_id,
                        message_id=user_sessions[chat_id]['status_messages'][player_id],
                        text=message_text
                    )
                    return
                except Exception as e:
                    logger.error(f"Error updating message: {e}")
        message = await context.bot.send_message(chat_id=chat_id, text=message_text)
        if chat_id not in user_sessions:
            user_sessions[chat_id] = {'tracked_players': {}, 'status_messages': {}}
        user_sessions[chat_id]['status_messages'][player_id] = message.message_id


async def monitor_players(chat_id: int, context: ContextTypes.DEFAULT_TYPE):
    logger.info(f"Starting monitoring for chat {chat_id}")
    
    async with aiohttp.ClientSession() as session:
        api = BattleMetricsAPI(API_KEY, session)
        
        while True:
            try:
                if chat_id not in user_sessions or not user_sessions[chat_id].get('tracked_players'):
                    logger.info(f"No players to monitor for chat {chat_id}, stopping task")
                    break
                    
                tracked_players = dict(user_sessions[chat_id].get('tracked_players', {}))
                logger.debug(f"Monitoring {len(tracked_players)} players for chat {chat_id}")
                
                # Проверяем каждого игрока параллельно
                tasks = []
                for player_id, server_id in tracked_players.items():
                    if chat_id not in user_sessions or player_id not in user_sessions[chat_id]['tracked_players']:
                        continue
                        
                    task = asyncio.create_task(check_player_status(chat_id, player_id, server_id, context, api))
                    tasks.append(task)
                
                if tasks:
                    await asyncio.gather(*tasks, return_exceptions=True)
                
                # Ждем перед следующей проверкой
                await asyncio.sleep(60)
                
            except asyncio.CancelledError:
                logger.info(f"Monitoring task for chat {chat_id} was cancelled")
                break
            except Exception as e:
                logger.error(f"Error in monitoring for chat {chat_id}: {e}")
                await asyncio.sleep(10)
        
        logger.info(f"Monitoring for chat {chat_id} stopped")


async def check_player_status(chat_id: int, player_id: str, server_id: str, context: ContextTypes.DEFAULT_TYPE, api: BattleMetricsAPI):
    """Проверяет статус одного игрока"""
    try:
        # НОВАЯ ЛОГИКА: Получаем текущий сервер игрока
        current_server_id, is_currently_online = await api.get_current_server_for_player(player_id)
        
        # Если не удалось определить сервер, пропускаем проверку
        if current_server_id is None:
            logger.warning(f"Could not determine current server for player {player_id}")
            return
            
        # Проверяем, изменился ли сервер
        stored_server_id = user_sessions[chat_id]['tracked_players'].get(player_id)
        
        # Если сервер изменился, обновляем его
        if stored_server_id != current_server_id:
            logger.info(f"Player {player_id} changed server from {stored_server_id} to {current_server_id}")
            user_sessions[chat_id]['tracked_players'][player_id] = current_server_id
            server_id = current_server_id  # Обновляем server_id для дальнейшей работы
            save_tracked_players_with_status()
            
            # Отправляем уведомление о смене сервера
            old_server_info = await api.get_server_info(stored_server_id) if stored_server_id else {"data": {"attributes": {"name": "неизвестный сервер"}}}
            new_server_info = await api.get_server_info(current_server_id)
            
            old_server_name = old_server_info.get("data", {}).get("attributes", {}).get("name", f"Сервер {stored_server_id}")
            new_server_name = new_server_info.get("data", {}).get("attributes", {}).get("name", f"Сервер {current_server_id}")
            
            player_info = await api.get_player_info(player_id)
            player_name = player_info.get("data", {}).get("attributes", {}).get("name", f"ID:{player_id}")
            
            current_time = datetime.now(TIMEZONE).strftime("%d.%m, %H:%M:%S")
            
            await context.bot.send_message(
                chat_id=chat_id,
                text=f"🔄 Игрок {player_name} сменил сервер!\n"
                     f"📤 Был на: {old_server_name}\n"
                     f"📥 Перешел на: {new_server_name}\n"
                     f"🕒 {current_time}"
            )
        
        # Теперь проверяем статус на текущем сервере
        if 'last_status' not in user_sessions[chat_id]:
            user_sessions[chat_id]['last_status'] = {}

        current_status = is_currently_online  # Используем уже полученный статус
        prev_status = user_sessions[chat_id]['last_status'].get(player_id)

        logger.info(f"Checking player {player_id}: prev={prev_status}, current={current_status}")

        if prev_status is None:
            # Первый раз - просто сохраняем текущий статус
            user_sessions[chat_id]['last_status'][player_id] = current_status
            logger.info(f"Set initial status for player {player_id}: {current_status}")
        elif prev_status != current_status:
            message_text = await get_player_status_message(player_id, server_id, api)
            if player_id in user_sessions[chat_id]['status_messages']:
                try:
                    await context.bot.edit_message_text(
                        chat_id=chat_id,
                        message_id=user_sessions[chat_id]['status_messages'][player_id],
                        text=message_text
                    )
                except Exception as e:
                    logger.error(f"Error updating message: {e}")
                    # Если не удалось обновить, отправляем новое сообщение
                    message = await context.bot.send_message(chat_id=chat_id, text=message_text)
                    user_sessions[chat_id]['status_messages'][player_id] = message.message_id
            
            player_info = await api.get_player_info(player_id)
            player_name = player_info.get("data", {}).get("attributes", {}).get("name", f"ID:{player_id}")
            event = "зашёл на сервер" if current_status else "вышел с сервера"
            current_time = datetime.now(TIMEZONE).strftime("%d.%m, %H:%M:%S")
            
            # Получаем имя сервера для уведомления
            server_info = await api.get_server_info(server_id)
            server_name = server_info.get("data", {}).get("attributes", {}).get("name", f"Сервер {server_id}")
            
            await context.bot.send_message(
                chat_id=chat_id,
                text=f"📢 Игрок {player_name} {event.upper()}!\n"
                     f"🌐 Сервер: {server_name}\n"
                     f"🕒 {current_time}"
            )
            user_sessions[chat_id]['last_status'][player_id] = current_status
            logger.info(f"Player {player_id} status changed: {prev_status} -> {current_status}")
            # Сохраняем обновленные статусы
            save_tracked_players_with_status()
            
        # Небольшая задержка между запросами для одного игрока
        await asyncio.sleep(2)
        
    except Exception as e:
        logger.error(f"Error checking player {player_id} status: {e}")


def save_tracked_players_with_status():
    """Сохраняет данные об отслеживаемых игроках с их текущими статусами"""
    try:
        # Создаем копию данных для сохранения с текущими статусами
        safe_data = {}
        for chat_id, session in user_sessions.items():
            safe_data[str(chat_id)] = {
                'tracked_players': session.get('tracked_players', {}),
                'status_messages': session.get('status_messages', {}),
                'last_status': session.get('last_status', {})  # Сохраняем текущие статусы
            }
        
        with open(DATA_FILE, 'w', encoding='utf-8') as f:
            json.dump(safe_data, f, indent=2, ensure_ascii=False)
        logger.info("Tracked players data with status saved")
    except Exception as e:
        logger.error(f"Error saving tracked players data with status: {e}")


def save_tracked_players():
    """Сохраняет данные об отслеживаемых игроках (без статусов для совместимости)"""
    save_tracked_players_with_status()


def load_tracked_players():
    if not os.path.exists(DATA_FILE):
        return []
    try:
        with open(DATA_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
            for chat_id, session in data.items():
                user_sessions[int(chat_id)] = {
                    'tracked_players': session.get('tracked_players', {}),
                    'status_messages': session.get('status_messages', {}),
                    'last_status': session.get('last_status', {})  # Загружаем сохраненные статусы
                }
        logger.info("Tracked players data with status loaded")
        return list(data.keys())  # Возвращаем список chat_id для запуска мониторинга
    except Exception as e:
        logger.error(f"Error loading tracked players data: {e}")
        return []


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if chat_id not in user_sessions:
        user_sessions[chat_id] = {'tracked_players': {}, 'status_messages': {}, 'last_status': {}}
    help_text = """
🤖 Бот для отслеживания игроков в Rust через BattleMetrics
Команды:
/start - Показать это сообщение
/track <ID игрока> - Начать отслеживание (без ID сервера)
/untrack <ID игрока> - Остановить отслеживание
/list - Показать отслеживаемых игроков
"""
    await update.message.reply_text(help_text)


@rate_limit(3)
async def track_player(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    try:
        if len(context.args) != 1:
            await update.message.reply_text("❌ Неправильный формат. Используйте: /track <ID игрока>")
            return
        player_id = context.args[0]
        if not player_id.isdigit():
            await update.message.reply_text("❌ ID игрока должно быть числом")
            return

        if chat_id not in user_sessions:
            user_sessions[chat_id] = {'tracked_players': {}, 'status_messages': {}, 'last_status': {}}

        if player_id in user_sessions[chat_id]['tracked_players']:
            await update.message.reply_text("⚠️ Этот игрок уже отслеживается.")
            return

        if len(user_sessions[chat_id]['tracked_players']) >= MAX_PLAYERS_PER_USER:
            await update.message.reply_text(f"🚫 Вы не можете отслеживать больше {MAX_PLAYERS_PER_USER} игроков.")
            return

        async with aiohttp.ClientSession() as session:
            api = BattleMetricsAPI(API_KEY, session)
            sessions = await api.get_player_sessions(player_id)

            if 'errors' in sessions:
                await update.message.reply_text("❌ Игрок не найден или нет данных о сессиях")
                return

            # ИСПРАВЛЕНО: Логика определения сервера
            # Теперь ищем последнюю активную сессию (где stop=null)
            # Если такой нет, ищем последнюю сессию по времени
            server_id = None
            latest_session = None
            latest_time = None
            
            for session_data in sessions.get("data", []):
                server_data = session_data["relationships"].get("server", {}).get("data")
                if server_data and "id" in server_data:
                    # Проверяем, активна ли сессия
                    stop_time = session_data["attributes"].get("stop")
                    start_time_str = session_data["attributes"].get("start")
                    
                    if stop_time is None:  # Активная сессия
                        server_id = server_data["id"]
                        break  # Нашли активную сессию, используем её
                    elif start_time_str:  # Неактивная сессия, проверяем время
                        try:
                            # Парсим время начала сессии
                            if '.' in start_time_str:
                                start_time_str = start_time_str.split('.')[0] + 'Z'
                            session_start = datetime.strptime(start_time_str, "%Y-%m-%dT%H:%M:%SZ")
                            
                            # Если это самая последняя сессия по времени
                            if latest_time is None or session_start > latest_time:
                                latest_time = session_start
                                latest_session = session_data
                        except Exception as e:
                            logger.error(f"Error parsing session time: {e}")
                            continue

            # Если не нашли активную сессию, используем последнюю по времени
            if server_id is None and latest_session is not None:
                server_data = latest_session["relationships"].get("server", {}).get("data")
                if server_data and "id" in server_data:
                    server_id = server_data["id"]

            if not server_id:
                await update.message.reply_text("❌ Не удалось определить сервер для игрока")
                return

            user_sessions[chat_id]['tracked_players'][player_id] = server_id
            save_tracked_players()

            # Отправляем сообщение о добавлении игрока
            message_text = await get_player_status_message(player_id, server_id, api)
            message = await update.message.reply_text(message_text)
            user_sessions[chat_id]['status_messages'][player_id] = message.message_id

            # Получаем текущий статус игрока и сохраняем его
            current_status = await api.is_player_online(player_id, server_id)
            user_sessions[chat_id]['last_status'][player_id] = current_status
            save_tracked_players_with_status()

            # Запускаем мониторинг если он еще не запущен
            if chat_id not in active_tasks or active_tasks[chat_id] is None or active_tasks[chat_id].done():
                task = asyncio.create_task(monitor_players(chat_id, context))
                active_tasks[chat_id] = task
                logger.info(f"Started monitoring task for chat {chat_id}")

    except Exception as e:
        logger.error(f"Error in track_player: {e}")
        await update.message.reply_text(f"❌ Ошибка: {str(e)}")


@rate_limit(3)
async def untrack_player(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    try:
        if not context.args:
            await update.message.reply_text("❌ Укажите ID игрока: /untrack <ID игрока>")
            return
        player_id = context.args[0]

        if chat_id not in user_sessions or player_id not in user_sessions[chat_id]['tracked_players']:
            await update.message.reply_text(f"ℹ️ Игрок {player_id} не найден в вашем списке отслеживаемых")
            return

        if player_id in user_sessions[chat_id]['status_messages']:
            try:
                await context.bot.delete_message(
                    chat_id=chat_id,
                    message_id=user_sessions[chat_id]['status_messages'].get(player_id)
                )
            except Exception as e:
                logger.error(f"Error deleting message: {e}")

        user_sessions[chat_id]['tracked_players'].pop(player_id, None)
        user_sessions[chat_id]['last_status'].pop(player_id, None)
        user_sessions[chat_id]['status_messages'].pop(player_id, None)

        save_tracked_players_with_status()

        await update.message.reply_text(f"✅ Игрок {player_id} больше не отслеживается")

        # Останавливаем мониторинг если больше нет игроков
        if not user_sessions[chat_id]['tracked_players']:
            if chat_id in active_tasks and active_tasks[chat_id]:
                active_tasks[chat_id].cancel()
                active_tasks[chat_id] = None
                logger.info(f"Cancelled monitoring task for chat {chat_id}")

    except Exception as e:
        logger.error(f"Error in untrack_player: {e}")
        await update.message.reply_text(f"❌ Ошибка: {str(e)}")


@rate_limit(3)
async def list_players(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    try:
        if chat_id not in user_sessions or not user_sessions[chat_id]['tracked_players']:
            await update.message.reply_text("ℹ️ У вас нет отслеживаемых игроков")
            return
            
        async with aiohttp.ClientSession() as session:
            api = BattleMetricsAPI(API_KEY, session)
            
            # Получаем информацию о всех игроках параллельно
            player_tasks = []
            server_tasks = []
            status_tasks = []
            time_tasks = []
            
            # НОВАЯ ЛОГИКА: Получаем текущие серверы для всех игроков
            current_server_tasks = []
            for player_id in user_sessions[chat_id]['tracked_players'].keys():
                current_server_tasks.append(asyncio.create_task(api.get_current_server_for_player(player_id)))
            
            # Ждем завершения запросов текущих серверов
            current_servers_results = await asyncio.gather(*current_server_tasks, return_exceptions=True)
            
            # Обновляем серверы в user_sessions если они изменились
            updated_players = {}
            for i, player_id in enumerate(user_sessions[chat_id]['tracked_players'].keys()):
                if i < len(current_servers_results) and not isinstance(current_servers_results[i], Exception):
                    current_server_id, _ = current_servers_results[i]
                    if current_server_id and current_server_id != user_sessions[chat_id]['tracked_players'][player_id]:
                        user_sessions[chat_id]['tracked_players'][player_id] = current_server_id
                        updated_players[player_id] = current_server_id
            
            # Если были обновления, сохраняем
            if updated_players:
                save_tracked_players_with_status()
                logger.info(f"Updated servers for players: {updated_players}")
            
            # Теперь получаем актуальную информацию
            for player_id, server_id in user_sessions[chat_id]['tracked_players'].items():
                player_tasks.append(asyncio.create_task(api.get_player_info(player_id)))
                server_tasks.append(asyncio.create_task(api.get_server_info(server_id)))
                status_tasks.append(asyncio.create_task(api.is_player_online(player_id, server_id)))
                time_tasks.append(asyncio.create_task(api.get_current_session_time(player_id, server_id)))
            
            # Ждем завершения всех запросов
            player_infos = await asyncio.gather(*player_tasks, return_exceptions=True)
            server_infos = await asyncio.gather(*server_tasks, return_exceptions=True)
            statuses = await asyncio.gather(*status_tasks, return_exceptions=True)
            times = await asyncio.gather(*time_tasks, return_exceptions=True)
            
            message = "📋 Ваши отслеживаемые игроки:\n"
            
            for i, (player_id, server_id) in enumerate(user_sessions[chat_id]['tracked_players'].items()):
                # Обрабатываем возможные ошибки
                player_info = player_infos[i] if i < len(player_infos) and not isinstance(player_infos[i], Exception) else {}
                server_info = server_infos[i] if i < len(server_infos) and not isinstance(server_infos[i], Exception) else {}
                status = statuses[i] if i < len(statuses) and not isinstance(statuses[i], Exception) else False
                session_time = times[i] if i < len(times) and not isinstance(times[i], Exception) else timedelta(0)
                
                player_name = "Неизвестный игрок"
                if isinstance(player_info, dict) and 'data' in player_info:
                    player_name = player_info.get("data", {}).get("attributes", {}).get("name", f"ID:{player_id}")
                
                server_name = f"Сервер {server_id}"
                if isinstance(server_info, dict) and 'data' in server_info:
                    server_name = server_info.get("data", {}).get("attributes", {}).get("name", f"Сервер {server_id}")
                    
                status_text = "🟢ОНЛАЙН🟢" if status else "🔴ОФФЛАЙН🔴"
                formatted_time = await format_duration(session_time) if isinstance(session_time, timedelta) else "0ч 0м"
                
                message += (
                    f"👤 {player_name}\n"
                    f"🆔 ID: {player_id}\n"
                    f"🌐 Сервер: {server_name}\n"
                    f"🔄 Статус: {status_text}\n"
                    f"⏱ Время на сервере: {formatted_time}\n"
                    f"──────────────────\n"
                )
                
            await update.message.reply_text(message)
        
    except Exception as e:
        logger.error(f"Error in list_players: {e}")
        await update.message.reply_text(f"❌ Ошибка: {str(e)}")


async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logger.error(f"Error in update processing: {context.error}")
    if update and update.effective_message:
        await update.effective_message.reply_text(f"❌ Произошла ошибка: {str(context.error)}")


def cleanup():
    """Корректное завершение работы"""
    logger.info("Shutting down...")
    
    # Отменяем все активные задачи
    for chat_id, task in active_tasks.items():
        if task and not task.done():
            task.cancel()
    
    # Сохраняем данные со статусами
    save_tracked_players_with_status()
    logger.info("Shutdown complete")


async def post_init(application: Application) -> None:
    """Функция, вызываемая после инициализации приложения"""
    # Загружаем данные
    chat_ids = load_tracked_players()
    for chat_id_str in chat_ids:
        chat_id = int(chat_id_str)
        if chat_id in user_sessions and user_sessions[chat_id].get('tracked_players'):
            # Запускаем мониторинг для этого чата
            if chat_id not in active_tasks or active_tasks[chat_id] is None or active_tasks[chat_id].done():
                task = asyncio.create_task(monitor_players(chat_id, application))
                active_tasks[chat_id] = task
                logger.info(f"Started monitoring task for existing chat {chat_id}")


def main():
    try:
        # Создаем приложение
        application = Application.builder().token(TELEGRAM_TOKEN).post_init(post_init).build()
        
        # Добавляем обработчики
        application.add_handler(CommandHandler("start", start))
        application.add_handler(CommandHandler("track", track_player))
        application.add_handler(CommandHandler("untrack", untrack_player))
        application.add_handler(CommandHandler("list", list_players))
        application.add_error_handler(error_handler)
        
        logger.info("Bot started...")
        application.run_polling(stop_signals=None)  # Отключаем автоматическую обработку сигналов
        
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
        cleanup()
    except Exception as e:
        logger.error(f"Error in main loop: {e}")
        cleanup()


if __name__ == "__main__":
    main()