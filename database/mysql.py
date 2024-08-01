import os
import asyncmy
from utils.logger import ModuleLogger
from sqlalchemy import create_engine, inspect
from sqlalchemy.orm import sessionmaker
from fastapi import Depends
from utils.config import ConfigUtil
from models.db_models import Base, Customer, User

logger = ModuleLogger(__name__).get_logger()

class MysqlClient:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(MysqlClient, cls).__new__(cls)
            cls._instance._initialize()
        return cls._instance

    def _initialize(self):
        if not hasattr(self, '_initialized'):
            self._initialized = True
            self.config_util = ConfigUtil()
            self.host = self.config_util.get_config('MYSQL', 'MYSQL_HOST')
            self.port = int(self.config_util.get_config('MYSQL', 'MYSQL_PORT'))
            self.user = self.config_util.get_config('MYSQL', 'MYSQL_USER')
            self.password = self.config_util.get_config('MYSQL', 'MYSQL_PASSWORD')
            self.db_name = self.config_util.get_config('MYSQL', 'MYSQL_DB_NAME')
            self.pool = None
            self.engine = None

    async def create_pool(self):
        self.pool = await asyncmy.create_pool(
            host=self.host,
            port=int(self.port),
            user=self.user,
            password=self.password,
            db=self.db_name,
            autocommit=True,
            connect_timeout=10
        )
        logger.info("Try to connect to database: %s", self.db_name)
        try:
            self.engine = create_engine(
                f'mysql+asyncmy://{self.user}:{self.password}@{self.host}:{self.port}/{self.db_name}'
            )
            if self.engine:
                logger.info("Created connection engine",
                            extra={"host": self.host, "port": self.port, "user": self.user, "db": self.db_name})
                self.Session = sessionmaker(bind=self.engine)
                logger.info("Created session maker...")
                
                inspector = inspect(self.engine)
                existing_tables = inspector.get_table_names()
                if not existing_tables:
                    Base.metadata.create_all(bind=self.engine)
                    logger.info("Created new tables")

            else:
                logger.error("Error creating connection engine.")
        except Exception as e:
            logger.error("Error connecting to database: %s", e)
            raise

    async def execute(self, query, args=None):
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute(query, args)
                    await conn.commit()
        except asyncmy.errors.OperationalError as e:
            logger.error(f"Error: {e}")
            logger.info("Reconnecting to the database...")
            await self.create_pool()
            return await self.fetch(query, args)
        except Exception as e:
            logger.error(f"Error: {e}")
            return []

    async def fetch(self, query, args=None):
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute(query, args)
                    return await cursor.fetchall()
        except asyncmy.errors.OperationalError as e:
            logger.error(f"Error: {e}")
            logger.info("Reconnecting to the database...")
            await self.create_pool()
            return await self.fetch(query, args)
        except Exception as e:
            logger.error(f"Error: {e}")
            return []

    async def close(self):
        if self.pool:
            logger.info("Closing connection pool...")
            self.pool.close()
            logger.info("Waiting for connection pool to close...")
            await self.pool.wait_closed()

def get_mysql_client():
    client = MysqlClient()
    return client
