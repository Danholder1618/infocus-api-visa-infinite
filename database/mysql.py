import aiomysql
import asyncio
import os
from dotenv import load_dotenv
from sshtunnel import SSHTunnelForwarder
import logging

load_dotenv()

class MySQLDatabase:
    _instance = None
    _lock = asyncio.Lock()

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super().__new__(cls)
        return cls._instance

    async def __init__(self):
        if not hasattr(self, "initialized"):
            await self.connect()
            self.initialized = True

    async def connect(self):
        try:
            db_host = os.getenv("MYSQL_HOST")
            db_port = int(os.getenv("MYSQL_PORT", 3306))
            db_user = os.getenv("MYSQL_USER")
            db_password = os.getenv("MYSQL_PASSWORD")
            db_database = os.getenv("MYSQL_DB")

            vidation_db_host = os.getenv("MYSQL_HOST_VIDATION_SERVICE")
            vidation_db_port = int(os.getenv("MYSQL_PORT_VIDATION_SERVICE", 3306))
            vidation_db_user = os.getenv("MYSQL_USER_VIDATION_SERVICE")
            vidation_db_password = os.getenv("MYSQL_PASSWORD_VIDATION_SERVICE")
            vidation_db_database = os.getenv("MYSQL_DB_VIDATION_SERVICE")

            use_ssh = os.getenv("USE_SSH") == "True"
            self.use_ssh = use_ssh

            if use_ssh:
                ssh_host = os.getenv("SSH_HOST")
                ssh_port = int(os.getenv("SSH_PORT", 22))
                ssh_user = os.getenv("SSH_USER")
                ssh_password = os.getenv("SSH_PASSWORD")

                self.ssh_tunnel = SSHTunnelForwarder(
                    (ssh_host, ssh_port),
                    ssh_username=ssh_user,
                    ssh_password=ssh_password,
                    remote_bind_address=(db_host, db_port)
                )
                self.ssh_tunnel.start()
                local_bind_port = self.ssh_tunnel.local_bind_port
                print(f"SSH tunnel established successfully: {local_bind_port}")

                self.pool = await aiomysql.create_pool(
                    host='127.0.0.1',
                    port=local_bind_port,
                    user=db_user,
                    password=db_password,
                    db=db_database,
                    autocommit=True
                )

                self.vidation_pool = await aiomysql.create_pool(
                    host='127.0.0.1',
                    port=local_bind_port,
                    user=vidation_db_user,
                    password=vidation_db_password,
                    db=vidation_db_database,
                    autocommit=True
                )
            else:
                self.pool = await aiomysql.create_pool(
                    host=db_host,
                    port=db_port,
                    user=db_user,
                    password=db_password,
                    db=db_database,
                    autocommit=True
                )

                self.vidation_pool = await aiomysql.create_pool(
                    host=vidation_db_host,
                    port=vidation_db_port,
                    user=vidation_db_user,
                    password=vidation_db_password,
                    db=vidation_db_database,
                    autocommit=True
                )
            logging.info("Success connecting to database")
        except Exception as e:
            logging.error(f"Error connecting to database: {e}")
            raise e

    async def close(self):
        self.pool.close()
        self.vidation_pool.close()
        if self.use_ssh:
            self.ssh_tunnel.stop()
        await self.pool.wait_closed()
        await self.vidation_pool.wait_closed()

    async def execute_query(self, query, params=None, as_dict=False, use_vidation_db=False):
        cursor_type = aiomysql.DictCursor if as_dict else aiomysql.Cursor
        pool = self.vidation_pool if use_vidation_db else self.pool

        async with pool.acquire() as conn:
            async with conn.cursor(cursor_type) as cur:
                print("Executing SQL query:")
                print("SQL:", cur.mogrify(query, params))
                await cur.execute(query, params)
                return await cur.fetchall()

    async def fetch_one(self, query, params=None, as_dict=False, use_vidation_db=False):
        return await self.execute_query(query, params, as_dict, use_vidation_db)

    async def fetch_all(self, query, params=None, as_dict=False, use_vidation_db=False):
        cursor_type = aiomysql.DictCursor if as_dict else aiomysql.Cursor
        pool = self.vidation_pool if use_vidation_db else self.pool
        async with pool.acquire() as conn:
            async with conn.cursor(cursor_type) as cur:
                await cur.execute(query, params)
                return await cur.fetchall()


database = MySQLDatabase()
