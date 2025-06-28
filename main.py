import os
import asyncio
from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.types import (
    InlineKeyboardMarkup, InlineKeyboardButton,
    ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove
)
from urllib.parse import urljoin
from loguru import logger
from db import init_db, save_org, is_in_db, get_random_orgs, get_user_count, get_org_count, get_city_count
from playwright.async_api import async_playwright

# Логирование
logger.add("logs.log", format="{time} | {level} | {message}", level="DEBUG", colorize=True)

load_dotenv()
bot = Bot(token=os.getenv("TG_TOKEN"))
dp = Dispatcher()

ADMIN_ID = int(os.getenv("ADMIN_ID"))
CHANNEL_ID = os.getenv("CHANNEL_ID")
users_state = {}
search_requests_count = 0  # счетчик поисковых запросов

# Проверка подписки
async def is_user_subscribed(bot, user_id):
    try:
        member = await bot.get_chat_member(chat_id=CHANNEL_ID, user_id=user_id)
        return member.status in ("member", "creator", "administrator")
    except:
        return False

# Команда /start
@dp.message(Command("start"))
async def start_command(message: types.Message):
    if not await is_user_subscribed(bot, message.from_user.id):
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Подписаться", url="https://t.me/send?start=SBmP_zOgHI7mY4ZWNi")],
            [InlineKeyboardButton(text="🔁 Проверить доступ", callback_data="check_access")]
        ])
        await message.answer("❗ Чтобы пользоваться ботом, подпишитесь на канал.", reply_markup=kb)
        return

    if message.from_user.id != ADMIN_ID:
        user_kb = ReplyKeyboardMarkup(keyboard=[
            [KeyboardButton(text="🔍 Начать поиск")]
        ], resize_keyboard=True)
        await message.answer(
            "👋 Я помогу найти бизнесы без сайтов - потенциальных клиентов.\n\n"
            "Нажми /find Москва кафе 10\n"
            "или воспользуйся кнопкой ниже для пошагового поиска.",
            reply_markup=user_kb
        )
    else:
        await message.answer(
            "👋 Админ, напиши /admin для входа в панель управления.",
            reply_markup=ReplyKeyboardRemove()
        )

# Обработка нажатия на "🔍 Начать поиск"
@dp.message(F.text == "🔍 Начать поиск")
async def handle_start_search(message: types.Message):
    await message.answer("✏️ Введите город:")
    users_state[message.from_user.id] = {"step": "city"}

@dp.message(lambda msg: msg.from_user.id in users_state)
async def step_handler(message: types.Message):
    state = users_state.get(message.from_user.id, {})
    if state["step"] == "city":
        state["city"] = message.text
        state["step"] = "type"
        await message.answer("💼 Введите тип бизнеса (например: кафе):")
    elif state["step"] == "type":
        state["type"] = message.text
        state["step"] = "count"
        await message.answer("🔢 Сколько результатов найти? (макс 50):")
    elif state["step"] == "count":
        try:
            count = min(max(int(message.text), 1), 50)
        except:
            await message.answer("❗ Введите число от 1 до 50.")
            return
        state["count"] = count
        await message.answer(f"🔍 Ищу '{state['type']}' в городе {state['city']}...")
        await search_and_send(message, state["city"], state["type"], state["count"])
        users_state.pop(message.from_user.id, None)

# Проверка доступа к боту
@dp.callback_query(lambda c: c.data == "check_access")
async def check_access(call: types.CallbackQuery):
    if await is_user_subscribed(bot, call.from_user.id):
        await call.message.answer("✅ Доступ подтверждён! Напишите /start заново.")
    else:
        await call.message.answer("⛔ Вы всё ещё не подписаны.")

# Команда /admin
@dp.message(Command("admin"))
async def handle_admin(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        await message.answer("⛔ У вас нет доступа к админ-панели.")
        return

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📊 Статистика", callback_data="stats")]
    ])
    await message.answer("🔧 Админ-панель", reply_markup=kb)

@dp.callback_query(F.data == "stats")
async def stats(call: types.CallbackQuery):
    # Получаем статистику из БД и из глобальных переменных
    user_count = await get_user_count()
    org_count = await get_org_count()
    city_count = await get_city_count()
    global search_requests_count

    text = (
        f"📊 Статистика бота:\n"
        f"👥 Количество пользователей: {user_count}\n"
        f"🏢 Количество уникальных организаций в базе: {org_count}\n"
        f"🌆 Количество городов в базе: {city_count}\n"
        f"🔎 Количество поисковых запросов за сессию: {search_requests_count}\n"
        f"\n"
        f"🕒 Логи поиска можно просмотреть в файле logs.log"
    )
    await call.message.answer(text)

# Команда /find
@dp.message(Command("find"))
async def handle_find(message: types.Message):
    try:
        _, city, biz_type, limit = message.text.split(" ", 3)
        limit = min(max(int(limit), 1), 50)
    except:
        await message.answer("❗ Неверный формат. Пример: /find Москва кафе 10")
        return
    await message.answer(f"🔍 Ищу '{biz_type}' в городе {city}...")
    global search_requests_count
    search_requests_count += 1
    await search_and_send(message, city, biz_type, limit)

# Команда /base
@dp.message(Command("base"))
async def handle_base(message: types.Message):
    try:
        _, city, count = message.text.split(" ", 2)
        count = int(count)
    except:
        await message.answer("❗ Формат: /base Москва 10")
        return

    records = await get_random_orgs(city, count)
    if not records:
        await message.answer("❌ В базе нет данных по этому городу.")
        return

    for title, address, phone, link in records:
        text = f"🏢 {title}\n📍 Адрес: {address}\n📞 Телефон: {phone}\n🌐 Сайт: ❌ Нет"
        btn = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🗺 Открыть в Яндекс Картах", url=link)]
        ])
        await message.answer(text, reply_markup=btn)

# Поиск и парсинг с логированием
async def search_and_send(message, city, biz_type, limit):
    count_found = 0
    logger.info(f"Начат поиск: город='{city}', тип='{biz_type}', лимит={limit}")

    async def on_item_found(org):
        nonlocal count_found
        count_found += 1
        logger.info(f"Найдена организация: {org['title']} | {org['address']} | {org['phone']} | {org['link']}")
        await message.answer(
            f"🏢 {org['title']}\n📍 Адрес: {org['address']}\n📞 Телефон: {org['phone']}\n🌐 Сайт: ❌ Нет",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🗺 Открыть в Яндекс Картах", url=org['link'])]
            ])
        )

    await parse_yandex_maps(city, biz_type, limit, on_item_found)

    if count_found == 0:
        logger.info("Поиск завершен: ничего не найдено или у всех есть сайты.")
        await message.answer("❌ Не найдено или у всех есть сайты.")
    else:
        logger.info(f"Поиск завершен: найдено {count_found} компаний без сайта.")
        await message.answer(f"✅ Найдено: {count_found} компаний без сайта.")

# Парсинг Яндекс Карт с логированием
async def parse_yandex_maps(city, biz_type, limit, on_item_found):
    base_url = "https://yandex.ru"
    url = f"{base_url}/maps/?text={city}+{biz_type}"

    logger.debug(f"Парсинг Яндекс.Карт по URL: {url}")
    async with async_playwright() as p:
        browser = await p.firefox.launch(headless=True)
        page = await browser.new_page()
        await page.goto(url)
        await page.wait_for_selector(".search-business-snippet-view__content", timeout=10000)

        found, seen = 0, set()
        while found < limit:
            cards = await page.query_selector_all(".search-business-snippet-view__content")
            if not cards:
                logger.debug("Нет бизнес-карт на странице или закончился список.")
                break

            for card in cards:
                if found >= limit:
                    break

                try:
                    link_el = await card.query_selector("a")
                    href = await link_el.get_attribute("href")
                    if not href:
                        continue
                    href = href.split("/reviews")[0]
                    full_url = urljoin(base_url, href)

                    if full_url in seen:
                        logger.debug(f"Пропускаем дубликат: {full_url}")
                        continue
                    if await is_in_db(full_url):
                        logger.debug(f"Организация уже в базе: {full_url}")
                        continue
                    seen.add(full_url)

                    detail_page = await browser.new_page()
                    await detail_page.goto(full_url)
                    await detail_page.wait_for_timeout(2000)

                    title_el = await detail_page.query_selector("h1")
                    title = (await title_el.text_content()).strip() if title_el else "Без названия"

                    addr_el = await detail_page.query_selector('div[class*="address"]')
                    address = (await addr_el.text_content()).strip() if addr_el else "Адрес не найден"

                    phone_el = await detail_page.query_selector('.card-phones-view__phone-number')
                    if not phone_el:
                        phone_el = await detail_page.query_selector('.orgpage-phones-view__phone-number')
                    phone = (await phone_el.text_content()).strip() if phone_el else "Не указан"

                    site_url = None
                    links = await detail_page.query_selector_all('a[href^="http"]')
                    for l in links:
                        href2 = await l.get_attribute("href")
                        if href2 and not any(d in href2 for d in ["yandex", "vk.com", "t.me", "instagram", "wa.me", "facebook", "ya.ru"]):
                            site_url = href2
                            break

                    await detail_page.close()

                    if site_url:
                        logger.debug(f"Организация имеет сайт, пропускаем: {title} - {site_url}")
                        continue

                    await save_org(city, title, address, phone, full_url)
                    await on_item_found({"title": title, "address": address, "phone": phone, "link": full_url})
                    found += 1
                except Exception as e:
                    logger.error(f"Ошибка при обработке организации: {e}")
                    continue

            await page.evaluate("""
                () => {
                    const container = document.querySelector("div.scroll__container");
                    if (container) container.scrollTop = container.scrollHeight;
                }
            """)
            await page.wait_for_timeout(1500)

        await browser.close()
        logger.debug(f"Парсинг завершен, найдено {found} организаций без сайта.")

if __name__ == "__main__":
    asyncio.run(init_db())
    logger.info("🚀 Бот запущен и готов к работе.")
    asyncio.run(dp.start_polling(bot))
