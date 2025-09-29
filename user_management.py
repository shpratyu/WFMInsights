import sqlite3
import bcrypt
from contextlib import contextmanager
import pandas as pd  # <-- This line is the required adjustment

DATABASE_NAME = 'wiseinsights_users.db'

@contextmanager
def get_db_connection():
    """Provides a context-managed database connection."""
    conn = sqlite3.connect(DATABASE_NAME)
    try:
        yield conn
    finally:
        conn.close()

def create_users_table():
    """Creates the users table if it does not exist."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS users (
                username TEXT PRIMARY KEY,
                password_hash TEXT NOT NULL,
                role TEXT NOT NULL,
                can_view_shrinkage INTEGER NOT NULL,
                can_view_volume INTEGER NOT NULL,
                can_view_capacity INTEGER NOT NULL,
                can_manage_schedules INTEGER NOT NULL
            )
        ''')
        conn.commit()

def add_user(username, password, role, permissions):
    """Adds a new user to the database with a hashed password."""
    password_hash = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
    with get_db_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute('''
                INSERT INTO users (username, password_hash, role, can_view_shrinkage, can_view_volume, can_view_capacity, can_manage_schedules)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (username, password_hash, role, permissions['can_view_shrinkage'], permissions['can_view_volume'], permissions['can_view_capacity'], permissions['can_manage_schedules']))
            conn.commit()
            return True
        except sqlite3.IntegrityError:
            return False # Username already exists

def update_user(username, new_password, role, permissions):
    """Updates an existing user's details and optionally their password."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        if new_password:
            password_hash = bcrypt.hashpw(new_password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
            cursor.execute('''
                UPDATE users SET password_hash=?, role=?, can_view_shrinkage=?, can_view_volume=?, can_view_capacity=?, can_manage_schedules=?
                WHERE username=?
            ''', (password_hash, role, permissions['can_view_shrinkage'], permissions['can_view_volume'], permissions['can_view_capacity'], permissions['can_manage_schedules'], username))
        else:
            cursor.execute('''
                UPDATE users SET role=?, can_view_shrinkage=?, can_view_volume=?, can_view_capacity=?, can_manage_schedules=?
                WHERE username=?
            ''', (role, permissions['can_view_shrinkage'], permissions['can_view_volume'], permissions['can_view_capacity'], permissions['can_manage_schedules'], username))
        conn.commit()

def delete_user(username):
    """Deletes a user from the database."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute('DELETE FROM users WHERE username=?', (username,))
        conn.commit()

def get_all_users():
    """Fetches all users from the database."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT username, role, can_view_shrinkage, can_view_volume, can_view_capacity, can_manage_schedules FROM users')
        columns = [desc[0] for desc in cursor.description]
        return pd.DataFrame(cursor.fetchall(), columns=columns)

def get_user_by_username(username):
    """Fetches a single user's record for authentication."""
    with get_db_connection() as conn:
        conn.row_factory = sqlite3.Row # Allows access by column name
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM users WHERE username=?', (username,))
        return cursor.fetchone()

def check_password_hash(password, password_hash):
    """Checks a plaintext password against a hash."""
    return bcrypt.checkpw(password.encode('utf-8'), password_hash.encode('utf-8'))