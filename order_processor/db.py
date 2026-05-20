import functools
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from config import DATABASE_URL


engine = create_async_engine(DATABASE_URL, echo=False)
async_session_maker = async_sessionmaker(engine, expire_on_commit=False)


async def _run_with_commit(func, self, session, args, kwargs):
    try:
        kwargs['session'] = session
        result = await func(self, *args, **kwargs)
        await session.commit()
        return result
    except Exception:
        await session.rollback()
        raise


def with_session():
    def decorator(func):
        @functools.wraps(func)
        async def wrapper(self, *args, **kwargs):
            session = kwargs.get("session")
            if not session:
                async with self._session_maker() as session:
                    return await _run_with_commit(func, self, session, args, kwargs)
            return await _run_with_commit(func, self, session, args, kwargs)
        return wrapper
    return decorator


def transactional(session_maker):
    def decorator(func):
        @functools.wraps(func)
        async def wrapper(self, *args, **kwargs):
            async with session_maker() as session:
                return await _run_with_commit(func, self, session, args, kwargs)
        return wrapper
    return decorator
