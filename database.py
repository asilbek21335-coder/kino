import asyncpg
from config import DATABASE_URL

pool = None

# ======================== Vaqt zonasi ========================
TZ = 'Asia/Tashkent'  # O'zbekiston vaqti (UTC+5)


async def init_db():
    global pool
    pool = await asyncpg.create_pool(
        DATABASE_URL,
        min_size=2,                            # doimiy ochiq connectionlar
        max_size=10,                           # maksimal parallel connection
        max_inactive_connection_lifetime=300,  # 5 daqiqada bo'sh connectionni yopish
        command_timeout=30,                    # har query uchun 30s limit
        timeout=10                             # connection olishga 10s kutish
    )

    async with pool.acquire() as conn:
        # ----- Foydalanuvchilar -----
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS users (
                user_id BIGINT PRIMARY KEY,
                first_start TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_activity TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                referred_by TEXT
            )
        ''')

        # ----- Videolar -----
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS videos (
                code TEXT PRIMARY KEY,
                file_id TEXT NOT NULL,
                description TEXT
            )
        ''')

        # ----- Referallar -----
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS referrals (
                code TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                count INTEGER DEFAULT 0
            )
        ''')

        # ----- Reklama -----
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS ads (
                id INTEGER PRIMARY KEY DEFAULT 1,
                content_type TEXT NOT NULL,
                file_id TEXT,
                text TEXT,
                caption TEXT,
                send_count INTEGER DEFAULT 0
            )
        ''')
        await conn.execute('''
            INSERT INTO ads (id, content_type, file_id, text, caption, send_count)
            VALUES (1, 'empty', NULL, NULL, NULL, 0)
            ON CONFLICT (id) DO NOTHING
        ''')

        # ----- Majburiy obuna -----
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS mandatory_subscriptions (
                id SERIAL PRIMARY KEY,
                type TEXT NOT NULL,
                identifier TEXT NOT NULL,
                limit_count INTEGER NOT NULL,
                current_count INTEGER DEFAULT 0,
                is_active INTEGER DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        try:
            await conn.execute('''
                ALTER TABLE mandatory_subscriptions 
                ADD COLUMN IF NOT EXISTS chat_id BIGINT
            ''')
        except:
            pass

        await conn.execute('''
            CREATE TABLE IF NOT EXISTS user_completed_subs (
                user_id BIGINT NOT NULL,
                sub_id INTEGER NOT NULL,
                completed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (user_id, sub_id)
            )
        ''')

        # ======================== INDEKSLAR (tezlik uchun) ========================
        await conn.execute('CREATE INDEX IF NOT EXISTS idx_users_last_activity ON users(last_activity DESC)')
        await conn.execute('CREATE INDEX IF NOT EXISTS idx_users_first_start ON users(first_start DESC)')
        await conn.execute('CREATE INDEX IF NOT EXISTS idx_users_referred_by ON users(referred_by)')
        await conn.execute('CREATE INDEX IF NOT EXISTS idx_user_completed_subs_user ON user_completed_subs(user_id)')
        await conn.execute('CREATE INDEX IF NOT EXISTS idx_mandatory_subs_active ON mandatory_subscriptions(is_active, current_count)')
        await conn.execute('CREATE INDEX IF NOT EXISTS idx_videos_code ON videos(code)')


# ======================== Foydalanuvchilar ========================
async def register_user_start(user_id, referral_code=None):
    async with pool.acquire() as conn:
        async with conn.transaction():
            exists = await conn.fetchval("SELECT 1 FROM users WHERE user_id = $1", user_id)
            if not exists:
                await conn.execute(
                    "INSERT INTO users (user_id, referred_by) VALUES ($1, $2)",
                    user_id, referral_code
                )
                if referral_code:
                    await conn.execute(
                        "UPDATE referrals SET count = count + 1 WHERE code = $1",
                        referral_code
                    )
            else:
                await conn.execute(
                    "UPDATE users SET last_activity = CURRENT_TIMESTAMP WHERE user_id = $1",
                    user_id
                )


async def update_last_activity(user_id: int):
    """Har qanday xabar/komanda kelganda last_activity ni yangilaydi"""
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE users SET last_activity = CURRENT_TIMESTAMP WHERE user_id = $1",
            user_id
        )


async def get_user_referral_count(user_id):
    """Foydalanuvchi qancha odam qo'shganini qaytaradi"""
    async with pool.acquire() as conn:
        count = await conn.fetchval(
            "SELECT COUNT(*) FROM users WHERE referred_by = $1::text",
            str(user_id)
        )
        ref_count = await conn.fetchval(
            "SELECT COALESCE(SUM(count), 0) FROM referrals WHERE code = $1::text",
            str(user_id)
        )
        return count + ref_count


async def get_total_users():
    async with pool.acquire() as conn:
        return await conn.fetchval("SELECT COUNT(*) FROM users")


async def get_today_users():
    """Bugun qo'shilganlar (O'zbekiston vaqti bo'yicha)"""
    async with pool.acquire() as conn:
        return await conn.fetchval(
            "SELECT COUNT(*) FROM users "
            "WHERE (first_start AT TIME ZONE 'UTC' AT TIME ZONE $1)::date "
            "= (CURRENT_TIMESTAMP AT TIME ZONE 'UTC' AT TIME ZONE $1)::date",
            TZ
        )


async def get_week_users():
    """Oxirgi 7 kunda qo'shilganlar (O'zbekiston vaqti bo'yicha)"""
    async with pool.acquire() as conn:
        return await conn.fetchval(
            "SELECT COUNT(*) FROM users "
            "WHERE first_start >= "
            "(CURRENT_TIMESTAMP AT TIME ZONE 'UTC' AT TIME ZONE $1) - INTERVAL '7 days'",
            TZ
        )


async def get_active_users_last_24h():
    """Oxirgi 24 soatda faol bo'lganlar"""
    async with pool.acquire() as conn:
        return await conn.fetchval(
            "SELECT COUNT(*) FROM users "
            "WHERE last_activity >= CURRENT_TIMESTAMP - INTERVAL '24 hours'"
        )


async def get_all_user_ids():
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT user_id FROM users")
        return [r["user_id"] for r in rows]


# ======================== Videolar ========================
async def add_video(code: str, file_id: str, description: str = ""):
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO videos (code, file_id, description) VALUES ($1, $2, $3) "
            "ON CONFLICT (code) DO UPDATE SET file_id=$2, description=$3",
            code, file_id, description
        )


async def get_video(code: str):
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT file_id, description FROM videos WHERE code = $1", code)
        return (row["file_id"], row["description"]) if row else None


async def delete_video(code: str):
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM videos WHERE code = $1", code)


async def list_all_videos():
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT code, description FROM videos ORDER BY code")
        return [(r["code"], r["description"]) for r in rows]


# ======================== Referallar ========================
async def create_referral(name, code):
    async with pool.acquire() as conn:
        await conn.execute("INSERT INTO referrals (code, name) VALUES ($1, $2)", code, name)


async def check_referral_code(code):
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT code FROM referrals WHERE code = $1", code)
        return row is not None


async def get_all_referrals():
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT code, name, count FROM referrals ORDER BY name")
        return [(r["code"], r["name"], r["count"]) for r in rows]


# ======================== Reklama ========================
async def set_ad(content_type, file_id=None, text=None, caption=None):
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM ads WHERE id = 1")
        await conn.execute(
            "INSERT INTO ads (id, content_type, file_id, text, caption, send_count) "
            "VALUES (1, $1, $2, $3, $4, 0)",
            content_type, file_id, text, caption
        )


async def get_ad():
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT content_type, file_id, text, caption, send_count FROM ads WHERE id = 1"
        )
        if row and row["content_type"] != "empty":
            return row
        return None


async def remove_ad():
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE ads SET content_type='empty', file_id=NULL, text=NULL, caption=NULL, "
            "send_count=0 WHERE id=1"
        )


async def increment_ad_count():
    async with pool.acquire() as conn:
        await conn.execute("UPDATE ads SET send_count = send_count + 1 WHERE id = 1")


# ======================== Majburiy obuna ========================
async def get_active_mandatory_subs():
    """Faqat faol va limiti tugamagan obunalar"""
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT id, type, identifier, limit_count, current_count, chat_id "
            "FROM mandatory_subscriptions "
            "WHERE is_active = 1 AND current_count < limit_count "
            "ORDER BY id"
        )
        return [
            {
                "id": r["id"],
                "type": r["type"],
                "identifier": r["identifier"],
                "limit": r["limit_count"],
                "count": r["current_count"],
                "chat_id": r["chat_id"]
            }
            for r in rows
        ]


async def is_user_completed_sub(user_id: int, sub_id: int) -> bool:
    async with pool.acquire() as conn:
        row = await conn.fetchval(
            "SELECT 1 FROM user_completed_subs WHERE user_id = $1 AND sub_id = $2",
            user_id, sub_id
        )
        return row is not None


async def mark_user_completed_sub(user_id: int, sub_id: int) -> bool:
    async with pool.acquire() as conn:
        async with conn.transaction():
            existing = await conn.fetchval(
                "SELECT 1 FROM user_completed_subs WHERE user_id = $1 AND sub_id = $2",
                user_id, sub_id
            )
            if existing:
                return False

            await conn.execute(
                "INSERT INTO user_completed_subs (user_id, sub_id) VALUES ($1, $2)",
                user_id, sub_id
            )
            await conn.execute(
                "UPDATE mandatory_subscriptions SET current_count = current_count + 1 WHERE id = $1",
                sub_id
            )

            row = await conn.fetchrow(
                "SELECT current_count, limit_count FROM mandatory_subscriptions WHERE id = $1",
                sub_id
            )
            if row and row["current_count"] >= row["limit_count"]:
                await conn.execute(
                    "UPDATE mandatory_subscriptions SET is_active = 0 WHERE id = $1",
                    sub_id
                )
                return True
            return False


async def set_user_completed_sub(user_id: int, sub_id: int, completed: bool = True):
    async with pool.acquire() as conn:
        if completed:
            await conn.execute(
                "INSERT INTO user_completed_subs (user_id, sub_id) VALUES ($1, $2) ON CONFLICT DO NOTHING",
                user_id, sub_id
            )
        else:
            await conn.execute(
                "DELETE FROM user_completed_subs WHERE user_id = $1 AND sub_id = $2",
                user_id, sub_id
            )


async def add_mandatory_subscription(sub_type: str, identifier: str, limit_count: int, chat_id: int = None):
    """Bir xil identifier mavjud bo'lsa, qayta qo'shmaydi."""
    async with pool.acquire() as conn:
        existing = await conn.fetchval(
            "SELECT id FROM mandatory_subscriptions WHERE identifier = $1",
            identifier
        )
        if existing:
            return existing
        return await conn.fetchval(
            "INSERT INTO mandatory_subscriptions (type, identifier, limit_count, chat_id) "
            "VALUES ($1, $2, $3, $4) RETURNING id",
            sub_type, identifier, limit_count, chat_id
        )


async def remove_mandatory_subscription(sub_id: int):
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM mandatory_subscriptions WHERE id = $1", sub_id)
        await conn.execute("DELETE FROM user_completed_subs WHERE sub_id = $1", sub_id)


async def list_mandatory_subscriptions():
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT id, type, identifier, limit_count, current_count, is_active, chat_id "
            "FROM mandatory_subscriptions ORDER BY id"
        )
        return rows
