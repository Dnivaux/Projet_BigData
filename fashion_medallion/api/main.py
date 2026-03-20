import os
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import duckdb
from fastapi import Depends, FastAPI, HTTPException, Query, status
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from jose import JWTError, jwt
from passlib.context import CryptContext

SECRET_KEY = os.getenv("API_SECRET_KEY", "change_me_in_prod")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = int(os.getenv("API_TOKEN_EXPIRE_MINUTES", "60"))
DATAMART_BASE_PATH = os.getenv("DATAMART_BASE_PATH", "./data/datamart")

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/token")

# Utilisateur de demo, a remplacer par un vrai stockage utilisateur.
FAKE_USER_DB = {
    os.getenv("API_USERNAME", "admin"): {
        "username": os.getenv("API_USERNAME", "admin"),
        "hashed_password": pwd_context.hash(os.getenv("API_PASSWORD", "admin123")),
    }
}

app = FastAPI(title="Fashion Datamart API", version="1.0.0")


def verify_password(plain_password: str, hashed_password: str) -> bool:
    return pwd_context.verify(plain_password, hashed_password)


def authenticate_user(username: str, password: str) -> dict[str, str] | None:
    user = FAKE_USER_DB.get(username)
    if not user:
        return None
    if not verify_password(password, user["hashed_password"]):
        return None
    return user


def create_access_token(data: dict[str, Any], expires_delta: timedelta | None = None) -> str:
    to_encode = data.copy()
    expire = datetime.now(timezone.utc) + (expires_delta or timedelta(minutes=15))
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)


async def get_current_user(token: str = Depends(oauth2_scheme)) -> dict[str, str]:
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Invalid authentication credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username = payload.get("sub")
        if username is None:
            raise credentials_exception
    except JWTError as exc:
        raise credentials_exception from exc

    user = FAKE_USER_DB.get(username)
    if user is None:
        raise credentials_exception
    return user


@app.post("/token")
async def login_for_access_token(form_data: OAuth2PasswordRequestForm = Depends()) -> dict[str, str]:
    user = authenticate_user(form_data.username, form_data.password)
    if not user:
        raise HTTPException(status_code=401, detail="Incorrect username or password")

    access_token = create_access_token(
        data={"sub": user["username"]},
        expires_delta=timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES),
    )
    return {"access_token": access_token, "token_type": "bearer"}


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/datamarts")
async def list_datamarts(_: dict[str, str] = Depends(get_current_user)) -> dict[str, list[str]]:
    base = Path(DATAMART_BASE_PATH)
    if not base.exists():
        return {"datamarts": []}
    names = sorted([p.name for p in base.iterdir() if p.is_dir()])
    return {"datamarts": names}


@app.get("/datamarts/{datamart_name}")
async def read_datamart(
    datamart_name: str,
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=500),
    _: dict[str, str] = Depends(get_current_user),
) -> dict[str, Any]:
    if not re.fullmatch(r"[A-Za-z0-9_\-]+", datamart_name):
        raise HTTPException(status_code=400, detail="Invalid datamart name")

    datamart_path = Path(DATAMART_BASE_PATH) / datamart_name
    if not datamart_path.exists():
        raise HTTPException(status_code=404, detail=f"Datamart '{datamart_name}' not found")

    parquet_glob = str(datamart_path / "**/*.parquet")
    offset = (page - 1) * page_size

    con = duckdb.connect(database=":memory:")
    try:
        total = con.execute(f"SELECT COUNT(*) FROM read_parquet('{parquet_glob}')").fetchone()[0]
        rows = con.execute(
            f"SELECT * FROM read_parquet('{parquet_glob}') LIMIT {page_size} OFFSET {offset}"
        ).fetchall()
        columns = [c[0] for c in con.description]
        data = [dict(zip(columns, row)) for row in rows]
    finally:
        con.close()

    return {
        "datamart": datamart_name,
        "page": page,
        "page_size": page_size,
        "total_rows": total,
        "total_pages": (total + page_size - 1) // page_size,
        "data": data,
    }
