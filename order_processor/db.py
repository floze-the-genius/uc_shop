import functools
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from config import DATABASE_URL


engine = create_async_engine(DATABASE_URL, echo=False)
async_session_maker = async_sessionmaker(engine, expire_on_commit=False)


def with_session():
    def decorator(func):
        @functools.wraps(func)
        async def wrapper(self, *args, **kwargs):
            async with self._session_maker() as session:
                try:
                    result = await func(self, session, *args, **kwargs)
                    await session.commit()
                    return result
                except Exception:
                    await session.rollback()
                    raise
        return wrapper
    return decorator
