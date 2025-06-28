import aiosqlite

DB_PATH = "database.db"

async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        # Таблица организаций
        await db.execute('''
            CREATE TABLE IF NOT EXISTS organizations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                city TEXT,
                title TEXT,
                address TEXT,
                phone TEXT,
                link TEXT UNIQUE
            )
        ''')

        # Таблица пользователей (если у вас её нет, создайте для учета количества пользователей)
        await db.execute('''
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY
            )
        ''')

        await db.commit()

async def save_org(city, title, address, phone, link):
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute('''
                INSERT OR IGNORE INTO organizations (city, title, address, phone, link)
                VALUES (?, ?, ?, ?, ?)
            ''', (city, title, address, phone, link))
            await db.commit()
    except Exception as e:
        print(f"[DB ERROR] {e}")

async def is_in_db(link):
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT 1 FROM organizations WHERE link = ?", (link,))
        return await cursor.fetchone() is not None

async def get_random_orgs(city, limit):
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute('''
            SELECT title, address, phone, link FROM organizations
            WHERE city = ?
            ORDER BY RANDOM()
            LIMIT ?
        ''', (city, limit))
        return await cursor.fetchall()

# Функция для добавления пользователя в базу (чтобы считать уникальных пользователей)
async def save_user(user_id: int):
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute('''
                INSERT OR IGNORE INTO users (user_id) VALUES (?)
            ''', (user_id,))
            await db.commit()
    except Exception as e:
        print(f"[DB ERROR] {e}")

# Получить количество уникальных пользователей
async def get_user_count():
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT COUNT(*) FROM users")
        result = await cursor.fetchone()
        return result[0] if result else 0

# Количество уникальных организаций
async def get_org_count():
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT COUNT(DISTINCT link) FROM organizations")
        result = await cursor.fetchone()
        return result[0] if result else 0

# Количество уникальных городов
async def get_city_count():
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT COUNT(DISTINCT city) FROM organizations")
        result = await cursor.fetchone()
        return result[0] if result else 0
