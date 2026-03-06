from fastapi import FastAPI

from src.api import router


def create_app() -> FastAPI:
    app = FastAPI(
        title="User Service API",
        description="Welcome to the User Service API documentation."
                    "Here you will be able to discover all of the "
                    "ways you can interact with the User API.",
        root_path="/api/users",
    )

    app.include_router(router)

    return app


app = create_app()
